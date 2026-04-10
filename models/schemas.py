from pydantic import BaseModel
from typing import Optional, List
from datetime import date
from enum import Enum


class TransactionType(str, Enum):
    debit = "debit"
    credit = "credit"


class Transaction(BaseModel):
    fecha: Optional[date] = None
    descripcion: str
    monto: float
    tipo: TransactionType
    categoria: Optional[str] = None


class UploadResponse(BaseModel):
    total: int
    transactions: List[Transaction]
