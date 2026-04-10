import io
import json
import os
import re
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
    "type": "debit",
    "category": "Restaurantes"
  }}
]

Reglas estrictas:
- date: siempre formato YYYY-MM-DD
- description: nombre del comercio o descripción limpia
- amount: siempre número positivo, sin símbolos de moneda
- type: "debit" para gastos, compras, retiros | "credit" para pagos, abonos, ingresos
- category: usa exactamente uno de estos nombres:
  Transporte, Gasolina, Restaurantes, Supermercado, Droguería / Salud,
  Medicina Prepagada, Suscripciones Digitales, Entretenimiento,
  Ropa / Moda, Ropa Deportiva, Educación, Educación Digital, Hogar,
  Cuidado Personal, Perfumería / Cosméticos, Viajes / Vacaciones,
  Vehículo / Mecánica, Parqueaderos, Salud / Bienestar,
  Recreación Hijos, Gastos Notariales / Legales, Seguros,
  Telefonía / Internet, Domicilios / Delivery, Empresa (Reembolsable),
  Impuestos / Comisiones Bancarias, Compras Sin Detalle
- Ignorar: filas de saldo, totales, encabezados, intereses bancarios
- Incluir: GMF 4x1000 como debit, description "GMF 4x1000", category "Impuestos / Comisiones Bancarias"
- NO incluir pagos a la tarjeta de crédito como gastos

Extracto bancario a analizar:
{texto}
"""


# ---------------------------------------------------------------------------
# XLS/XLSX parsing with pandas (no Claude)
# ---------------------------------------------------------------------------

def _find_header_row(df: pd.DataFrame) -> int:
    """Return the row index that looks like a column-header row."""
    header_keywords = {
        "fecha", "date", "descripcion", "concepto", "valor", "monto",
        "debito", "credito", "cargo", "abono", "detalle", "importe",
        "transaccion", "movimiento", "referencia",
    }
    for idx in range(min(20, len(df))):
        row = df.iloc[idx]
        matches = 0
        for v in row:
            if pd.notna(v):
                cell = str(v).lower().strip()
                if any(kw in cell for kw in header_keywords):
                    matches += 1
        if matches >= 2:
            return idx
    return 0


def _find_col(columns: List[str], keywords: List[str]) -> Optional[str]:
    """Return the first column name that contains any of the keywords."""
    for col in columns:
        col_lower = col.lower()
        for kw in keywords:
            if kw in col_lower:
                return col
    return None


def _safe_float(value) -> float:
    """Convert a spreadsheet cell to float, handling Spanish/English formats."""
    if value is None:
        return 0.0
    try:
        if pd.isna(value):
            return 0.0
    except (TypeError, ValueError):
        pass
    if isinstance(value, (int, float)):
        v = float(value)
        return 0.0 if v != v else v  # guard NaN
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return 0.0
    negative = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    s = re.sub(r"[^\d.,\-]", "", s)
    if not s:
        return 0.0
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")  # Spanish: 1.234,56
        else:
            s = s.replace(",", "")                    # English:  1,234.56
    elif "," in s:
        parts = s.split(",")
        if len(parts) == 2 and len(parts[-1]) <= 2:
            s = s.replace(",", ".")   # decimal comma
        else:
            s = s.replace(",", "")    # thousands comma
    try:
        result = float(s)
        return -result if negative else result
    except ValueError:
        return 0.0


def _safe_date(value) -> Optional[date]:
    """Convert a spreadsheet cell to a date object."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_xls(file_bytes: bytes) -> List[Transaction]:
    """Parse XLS/XLSX directly with pandas — Claude is NOT used here."""
    engines = ["openpyxl", "xlrd"]

    # First pass: read everything as strings to locate the header row
    raw = None
    for engine in engines:
        try:
            raw = pd.read_excel(io.BytesIO(file_bytes), engine=engine, header=None, dtype=str)
            break
        except Exception:
            continue

    if raw is None or raw.empty:
        print("[XLS] Could not read file with any engine")
        return []

    header_idx = _find_header_row(raw)
    print(f"[XLS] Header row detected at index {header_idx}")

    # Second pass: read with correct header so pandas parses dates/numbers
    df = None
    for engine in engines:
        try:
            df = pd.read_excel(io.BytesIO(file_bytes), engine=engine, header=header_idx)
            break
        except Exception:
            continue

    if df is None or df.empty:
        return []

    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all")
    cols = list(df.columns)
    print(f"[XLS] Columns: {cols}")

    date_col = _find_col(cols, ["fecha", "date", "f.valor", "f.proceso", "f.transaccion"])
    desc_col = _find_col(cols, ["descripcion", "concepto", "detalle", "description", "referencia", "oficina", "nombre"])
    debit_col = _find_col(cols, ["debito", "cargo", "egreso", "retiro", "salida"])
    credit_col = _find_col(cols, ["credito", "abono", "ingreso", "deposito", "entrada"])
    amount_col = _find_col(cols, ["valor", "monto", "importe", "amount"])

    if not date_col:
        print("[XLS] No date column found")
        return []
    if not desc_col:
        print("[XLS] No description column found")
        return []

    print(f"[XLS] date={date_col}, desc={desc_col}, debit={debit_col}, credit={credit_col}, amount={amount_col}")

    transactions: List[Transaction] = []
    for _, row in df.iterrows():
        fecha = _safe_date(row.get(date_col))
        if not fecha:
            continue

        desc = str(row.get(desc_col, "")).strip()
        if not desc or desc.lower() in ("nan", "none", ""):
            continue

        if debit_col and credit_col:
            debit_val = abs(_safe_float(row.get(debit_col, 0)))
            credit_val = abs(_safe_float(row.get(credit_col, 0)))
            if debit_val > 0:
                monto, tipo = debit_val, TransactionType.debit
            elif credit_val > 0:
                monto, tipo = credit_val, TransactionType.credit
            else:
                continue
        elif amount_col:
            raw_amount = _safe_float(row.get(amount_col, 0))
            if raw_amount == 0:
                continue
            if raw_amount < 0:
                monto, tipo = abs(raw_amount), TransactionType.credit
            else:
                monto, tipo = raw_amount, TransactionType.debit
        else:
            continue

        transactions.append(
            Transaction(
                fecha=fecha,
                descripcion=desc,
                monto=monto,
                tipo=tipo,
                categoria=None,
            )
        )

    print(f"[XLS] Extracted {len(transactions)} transactions with pandas")
    return transactions


# ---------------------------------------------------------------------------
# PDF helpers
# ---------------------------------------------------------------------------

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

        categoria = item.get("category") or item.get("categoria") or None
        if categoria:
            categoria = str(categoria).strip() or None

        transactions.append(
            Transaction(
                fecha=fecha,
                descripcion=str(item.get("description", "")).strip(),
                monto=monto,
                tipo=tipo,
                categoria=categoria,
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

    # XLS/XLSX: parse directly with pandas — Claude is used only for categorization
    if ext in (".xls", ".xlsx"):
        return _parse_xls(file_bytes)

    # PDF: convert to text then send to Claude for parsing
    if extracted_text:
        text = extracted_text
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
