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


def _get_category_id(client: Client, category_name: str) -> Optional[str]:
    result = (
        client.table("categories")
        .select("id")
        .eq("name", category_name)
        .limit(1)
        .execute()
    )
    if result.data:
        return result.data[0]["id"]
    return None


def save_transactions(
    transactions: List[Transaction],
    user_id: str,
    source_file: str,
) -> List[dict]:
    client = get_client()

    # Cache category lookups to avoid N+1 queries
    category_cache: dict = {}

    rows = []
    for t in transactions:
        cat_name = t.categoria or "Otros"
        if cat_name not in category_cache:
            category_cache[cat_name] = _get_category_id(client, cat_name)

        row = {
            "user_id": user_id,
            "account_id": None,
            "date": t.fecha.strftime("%Y-%m-%d") if t.fecha else None,
            "description": t.descripcion,
            "amount": float(t.monto),
            "type": t.tipo.value,
            "category_id": category_cache[cat_name],
            "source_file": source_file,
            "month": int(t.fecha.month) if t.fecha else None,
            "year": int(t.fecha.year) if t.fecha else None,
        }
        rows.append(row)

    result = client.table("transactions").insert(rows).execute()
    return result.data
