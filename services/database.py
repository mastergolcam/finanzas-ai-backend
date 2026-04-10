import os
from typing import List, Optional

from dotenv import load_dotenv
from supabase import create_client, Client

from models.schemas import Transaction

load_dotenv()

_client: Optional[Client] = None


def get_client() -> Client:
    global _client
    if _client is None:
        url = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL y SUPABASE_KEY deben estar definidas en el .env")
        _client = create_client(url, key)
    return _client


def _build_category_map(client: Client, user_id: str) -> dict:
    result = client.table("categories").select("id,name").eq("user_id", user_id).execute()
    return {c["name"].lower(): c["id"] for c in result.data}


def _resolve_category_id(cat_map: dict, categoria: Optional[str]) -> Optional[str]:
    if not categoria:
        return None

    cat_lower = categoria.lower()

    # Exact match
    category_id = cat_map.get(cat_lower)
    if category_id:
        return category_id

    # Partial match fallback
    for cat_name, cat_id in cat_map.items():
        if cat_name in cat_lower or cat_lower in cat_name:
            return cat_id

    return None


def save_transactions(
    transactions: List[Transaction],
    user_id: str,
    source_file: str,
) -> List[dict]:
    client = get_client()
    cat_map = _build_category_map(client, user_id)

    rows = []
    for t in transactions:
        row = {
            "user_id": user_id,
            "account_id": None,
            "date": t.fecha.strftime("%Y-%m-%d") if t.fecha else None,
            "description": t.descripcion,
            "amount": float(t.monto),
            "type": t.tipo.value,
            "category_id": _resolve_category_id(cat_map, t.categoria),
            "source_file": source_file,
            "month": int(t.fecha.month) if t.fecha else None,
            "year": int(t.fecha.year) if t.fecha else None,
        }
        rows.append(row)

    result = client.table("transactions").insert(rows).execute()
    return result.data
