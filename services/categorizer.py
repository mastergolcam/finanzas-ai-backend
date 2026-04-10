import json
import os
import re
from typing import List

import anthropic
from dotenv import load_dotenv

from models.schemas import Transaction

load_dotenv()

CATEGORIES = [
    "Alimentación",
    "Transporte",
    "Entretenimiento",
    "Salud",
    "Educación",
    "Vivienda",
    "Servicios públicos",
    "Ropa y accesorios",
    "Tecnología",
    "Ingresos",
    "Transferencias",
    "Otros",
]

BATCH_SIZE = 50  # Max transactions per Claude request


def categorize_transactions(transactions: List[Transaction]) -> List[Transaction]:
    if not transactions:
        return transactions

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    all_categories: List[str] = []

    for i in range(0, len(transactions), BATCH_SIZE):
        batch = transactions[i : i + BATCH_SIZE]
        categories = _categorize_batch(client, batch)
        all_categories.extend(categories)

    for transaction, category in zip(transactions, all_categories):
        transaction.categoria = category

    return transactions


def _categorize_batch(client: anthropic.Anthropic, batch: List[Transaction]) -> List[str]:
    descriptions = [t.descripcion for t in batch]
    categories_str = "\n".join(f"- {c}" for c in CATEGORIES)

    prompt = f"""Eres un asistente experto en finanzas personales. Tu tarea es categorizar transacciones bancarias.

Categorías disponibles:
{categories_str}

Transacciones a categorizar (en orden):
{json.dumps(descriptions, ensure_ascii=False, indent=2)}

Reglas:
- Asigna exactamente una categoría por transacción, en el mismo orden.
- Usa "Ingresos" para salarios, transferencias recibidas, reembolsos.
- Usa "Transferencias" para movimientos entre cuentas propias.
- Usa "Otros" cuando no encaje en ninguna categoría anterior.
- Responde ÚNICAMENTE con un JSON array de strings. Sin explicaciones. Sin markdown.

Ejemplo de respuesta: ["Alimentación", "Transporte", "Ingresos"]"""

    message = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text.strip()

    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = re.sub(r"```[a-z]*\n?", "", raw).strip("` \n")

    try:
        result = json.loads(raw)
        if isinstance(result, list) and len(result) == len(batch):
            return result
    except (json.JSONDecodeError, ValueError):
        pass

    # Fallback: return "Otros" for each transaction in the batch
    return ["Otros"] * len(batch)


