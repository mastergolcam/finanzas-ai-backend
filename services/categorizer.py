import json
import os
import re
from typing import List

import anthropic
from dotenv import load_dotenv

from models.schemas import Transaction

load_dotenv()

CATEGORIES = [
    "Transporte",
    "Gasolina",
    "Restaurantes",
    "Supermercado",
    "Droguería / Salud",
    "Medicina Prepagada",
    "Suscripciones Digitales",
    "Entretenimiento",
    "Ropa / Moda",
    "Ropa Deportiva",
    "Educación",
    "Educación Digital",
    "Hogar",
    "Cuidado Personal",
    "Perfumería / Cosméticos",
    "Viajes / Vacaciones",
    "Vehículo / Mecánica",
    "Parqueaderos",
    "Salud / Bienestar",
    "Recreación Hijos",
    "Gastos Notariales / Legales",
    "Seguros",
    "Telefonía / Internet",
    "Domicilios / Delivery",
    "Empresa (Reembolsable)",
    "Impuestos / Comisiones Bancarias",
    "Compras Sin Detalle",
]

BATCH_SIZE = 50  # Max transactions per Claude request


def categorize_transactions(transactions: List[Transaction]) -> List[Transaction]:
    if not transactions:
        return transactions

    api_key = os.environ.get("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    print(f"API Key loaded: {api_key[:10] if api_key else 'NOT FOUND'}")
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
- Usa "Restaurantes" para comidas, cafeterías, domicilios de comida.
- Usa "Domicilios / Delivery" para Rappi, iFood, servicios de entrega.
- Usa "Suscripciones Digitales" para Netflix, Spotify, apps, software.
- Usa "Telefonía / Internet" para planes de celular, internet, TV.
- Usa "Impuestos / Comisiones Bancarias" para GMF, comisiones, cargos bancarios.
- Usa "Compras Sin Detalle" cuando no hay información suficiente para categorizar.
- Responde ÚNICAMENTE con un JSON array de strings con el campo "categoria". Sin explicaciones. Sin markdown.

Ejemplo de respuesta: ["Restaurantes", "Transporte", "Suscripciones Digitales"]"""

    message = client.messages.create(
        model="claude-sonnet-4-6",
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


