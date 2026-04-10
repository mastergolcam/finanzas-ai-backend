import io
import json
import os
from datetime import date, datetime
from typing import List, Optional

import anthropic
import pandas as pd
import pdfplumber
from dotenv import load_dotenv

from models.schemas import Transaction, TransactionType

load_dotenv()

PDF_DEBUG_PATH = "/tmp/pdf_debug.txt"

PROMPT = """Eres un experto en extractos bancarios de cualquier país.
Analiza este extracto bancario y extrae TODAS las transacciones.

Devuelve ÚNICAMENTE un JSON array válido, sin explicaciones, sin markdown, sin texto extra.
Formato exacto:
[
  {{
    "date": "YYYY-MM-DD",
    "description": "descripción del comercio o transacción",
    "amount": 12345.67,
    "type": "debit"
  }}
]

Reglas estrictas:
- date: siempre formato YYYY-MM-DD
- description: nombre del comercio o descripción limpia
- amount: siempre número positivo, sin símbolos de moneda
- type: "debit" para gastos, compras, retiros | "credit" para pagos, abonos, ingresos
- Ignorar: filas de saldo, totales, encabezados, intereses bancarios
- Incluir: GMF 4x1000 como debit con description "GMF 4x1000"
- NO incluir pagos a la tarjeta de crédito como gastos

Extracto bancario a analizar:
{texto}
"""


def _xls_to_text(file_bytes: bytes) -> str:
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")
    except Exception:
        df = pd.read_excel(io.BytesIO(file_bytes), engine="xlrd")

    df = df.dropna(how="all")
    lines = ["\t".join(str(v) for v in df.columns)]
    for _, row in df.iterrows():
        lines.append("\t".join(str(v) for v in row))
    return "\n".join(lines)


def _pdf_to_text(file_bytes: bytes) -> str:
    page_texts = []
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            page_texts.append(page.extract_text() or "")
    return "\n".join(page_texts)


CHUNK_SIZE = 3000


def _parse_claude_response(raw: str) -> List[dict]:
    # 1. Clean markdown fences
    cleaned = raw.replace("```json", "").replace("```", "").strip()

    print(f"Respuesta Claude primeros 500 chars: {cleaned[:500]}")

    # 2. Try direct parse
    try:
        return json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        pass

    # 3. Extract array with regex fallback
    import re
    match = re.search(r"\[.*\]", cleaned, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except (json.JSONDecodeError, ValueError):
            pass

    return []


def _items_to_transactions(items: List[dict]) -> List[Transaction]:
    transactions = []
    for item in items:
        raw_date = item.get("date", "")
        try:
            fecha = datetime.strptime(raw_date, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            fecha = None

        raw_type = str(item.get("type", "debit")).lower().strip()
        tipo = TransactionType.credit if raw_type == "credit" else TransactionType.debit

        try:
            monto = float(item.get("amount", 0))
        except (ValueError, TypeError):
            monto = 0.0

        if monto <= 0:
            continue

        transactions.append(
            Transaction(
                fecha=fecha,
                descripcion=str(item.get("description", "")).strip(),
                monto=monto,
                tipo=tipo,
            )
        )
    return transactions


def _call_claude_chunk(client: anthropic.Anthropic, text: str) -> List[Transaction]:
    prompt = PROMPT.format(texto=text)

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text
    items = _parse_claude_response(raw)

    if not items:
        print("Reintentando chunk...")
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        items = _parse_claude_response(message.content[0].text)

    return _items_to_transactions(items)


def _split_chunks(text: str, chunk_size: int = CHUNK_SIZE) -> List[str]:
    """Split text into chunks at newline boundaries to avoid cutting mid-transaction."""
    chunks = []
    lines = text.splitlines(keepends=True)
    current = []
    current_len = 0

    for line in lines:
        if current_len + len(line) > chunk_size and current:
            chunks.append("".join(current))
            current = []
            current_len = 0
        current.append(line)
        current_len += len(line)

    if current:
        chunks.append("".join(current))

    return chunks


def parse_file(
    file_bytes: bytes,
    filename: str,
    extracted_text: Optional[str] = None,
) -> List[Transaction]:
    ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext in (".xls", ".xlsx"):
        text = _xls_to_text(file_bytes)
    elif extracted_text:
        text = extracted_text
        lines = text.splitlines()
        with open(PDF_DEBUG_PATH, "w", encoding="utf-8") as f:
            f.write("\n".join(lines[:50]))
    else:
        text = _pdf_to_text(file_bytes)
        lines = text.splitlines()
        with open(PDF_DEBUG_PATH, "w", encoding="utf-8") as f:
            f.write("\n".join(lines[:50]))

    print(f"Texto enviado a Claude (primeros 300 chars): {text[:300]}")

    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    chunks = _split_chunks(text)
    print(f"Total chunks: {len(chunks)}")

    all_transactions: List[Transaction] = []
    for i, chunk in enumerate(chunks):
        print(f"Procesando chunk {i + 1}/{len(chunks)} ({len(chunk)} chars)")
        all_transactions.extend(_call_claude_chunk(client, chunk))

    return all_transactions
