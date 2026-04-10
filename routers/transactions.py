from typing import List, Optional

from fastapi import APIRouter, Query

from models.schemas import Transaction, TransactionType

router = APIRouter()

# In-memory store — replace with Supabase queries when DB is configured
_transactions: List[Transaction] = []


def store_transactions(transactions: List[Transaction]) -> None:
    """Persist transactions. Called after a successful upload."""
    _transactions.extend(transactions)


@router.get("/transactions", response_model=List[Transaction])
async def get_transactions(
    tipo: Optional[TransactionType] = Query(None, description="Filtrar por tipo: débito o crédito"),
    categoria: Optional[str] = Query(None, description="Filtrar por categoría"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    results = _transactions

    if tipo:
        results = [t for t in results if t.tipo == tipo]
    if categoria:
        results = [t for t in results if t.categoria and categoria.lower() in t.categoria.lower()]

    return results[offset : offset + limit]


@router.get("/transactions/summary")
async def get_summary():
    if not _transactions:
        return {"total_transacciones": 0, "total_debitos": 0.0, "total_creditos": 0.0, "categorias": {}}

    total_debitos = sum(t.monto for t in _transactions if t.tipo == TransactionType.debit)
    total_creditos = sum(t.monto for t in _transactions if t.tipo == TransactionType.credit)

    categorias: dict = {}
    for t in _transactions:
        cat = t.categoria or "Sin categoría"
        categorias.setdefault(cat, {"count": 0, "total": 0.0})
        categorias[cat]["count"] += 1
        categorias[cat]["total"] += t.monto

    return {
        "total_transacciones": len(_transactions),
        "total_debitos": round(total_debitos, 2),
        "total_creditos": round(total_creditos, 2),
        "categorias": categorias,
    }
