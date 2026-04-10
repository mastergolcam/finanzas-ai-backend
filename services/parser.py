import io
import re
from datetime import date, datetime
from typing import List, Optional

import pandas as pd
import pdfplumber

from models.schemas import Transaction, TransactionType


# Column name aliases for auto-detection (lowercase)
DATE_ALIASES = {"fecha", "date", "día", "dia", "f.valor", "f.transac"}
DESC_ALIASES = {"descripcion", "descripción", "concepto", "detalle", "descripci", "detail", "glosa"}
AMOUNT_ALIASES = {"monto", "valor", "importe", "amount", "total"}
DEBIT_ALIASES = {"debito", "débito", "cargo", "egreso", "salida", "debit"}
CREDIT_ALIASES = {"credito", "crédito", "abono", "ingreso", "entrada", "credit"}
TYPE_ALIASES = {"tipo", "type", "movimiento", "operacion", "operación"}


def _normalize(text: str) -> str:
    return str(text).strip().lower()


def _find_col(columns: List[str], aliases: set) -> Optional[str]:
    for col in columns:
        if _normalize(col) in aliases:
            return col
    # Partial match fallback
    for col in columns:
        norm = _normalize(col)
        for alias in aliases:
            if alias in norm:
                return col
    return None


def _parse_date(value) -> Optional[date]:
    if pd.isna(value) if not isinstance(value, str) else not value:
        return None
    if isinstance(value, (datetime, date)):
        return value.date() if isinstance(value, datetime) else value
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(value).strip(), fmt).date()
        except ValueError:
            continue
    return None


def _parse_amount(value) -> float:
    if pd.isna(value) if not isinstance(value, str) else not value:
        return 0.0
    cleaned = re.sub(r"[^\d,.\-]", "", str(value))
    # Handle comma as decimal separator
    if cleaned.count(",") == 1 and cleaned.count(".") == 0:
        cleaned = cleaned.replace(",", ".")
    elif cleaned.count(",") >= 1 and cleaned.count(".") >= 1:
        # e.g. 1.234,56 -> 1234.56
        cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_xls(file_bytes: bytes) -> List[Transaction]:
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")
    except Exception:
        df = pd.read_excel(io.BytesIO(file_bytes), engine="xlrd")

    df.columns = [str(c) for c in df.columns]
    cols = df.columns.tolist()

    date_col = _find_col(cols, DATE_ALIASES)
    desc_col = _find_col(cols, DESC_ALIASES)
    amount_col = _find_col(cols, AMOUNT_ALIASES)
    debit_col = _find_col(cols, DEBIT_ALIASES)
    credit_col = _find_col(cols, CREDIT_ALIASES)
    type_col = _find_col(cols, TYPE_ALIASES)

    transactions = []
    for _, row in df.iterrows():
        descripcion = str(row[desc_col]).strip() if desc_col else "Sin descripción"
        if not descripcion or descripcion.lower() in ("nan", "none", ""):
            continue

        fecha = _parse_date(row[date_col]) if date_col else None

        # Determine amount and type
        if debit_col and credit_col:
            debit_val = _parse_amount(row[debit_col])
            credit_val = _parse_amount(row[credit_col])
            if debit_val and debit_val > 0:
                monto = debit_val
                tipo = TransactionType.debit
            elif credit_val and credit_val > 0:
                monto = credit_val
                tipo = TransactionType.credit
            else:
                continue
        elif amount_col:
            monto = _parse_amount(row[amount_col])
            if type_col:
                raw_type = _normalize(str(row[type_col]))
                tipo = (
                    TransactionType.credit
                    if any(a in raw_type for a in CREDIT_ALIASES)
                    else TransactionType.debit
                )
            else:
                tipo = TransactionType.credit if monto >= 0 else TransactionType.debit
            monto = abs(monto)
        else:
            continue

        transactions.append(
            Transaction(fecha=fecha, descripcion=descripcion, monto=monto, tipo=tipo)
        )

    return transactions


def parse_pdf(file_bytes: bytes) -> List[Transaction]:
    transactions = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2:
                    continue

                # Use first row as header
                headers = [str(h).strip() if h else "" for h in table[0]]

                date_idx = next(
                    (i for i, h in enumerate(headers) if _normalize(h) in DATE_ALIASES), None
                )
                desc_idx = next(
                    (i for i, h in enumerate(headers) if _normalize(h) in DESC_ALIASES), None
                )
                amount_idx = next(
                    (i for i, h in enumerate(headers) if _normalize(h) in AMOUNT_ALIASES), None
                )
                debit_idx = next(
                    (i for i, h in enumerate(headers) if _normalize(h) in DEBIT_ALIASES), None
                )
                credit_idx = next(
                    (i for i, h in enumerate(headers) if _normalize(h) in CREDIT_ALIASES), None
                )

                for row in table[1:]:
                    if not row or all(not cell for cell in row):
                        continue

                    def cell(idx):
                        return row[idx] if idx is not None and idx < len(row) else None

                    descripcion = str(cell(desc_idx)).strip() if desc_idx is not None else ""
                    if not descripcion or descripcion.lower() in ("nan", "none", ""):
                        continue

                    fecha = _parse_date(cell(date_idx)) if date_idx is not None else None

                    if debit_idx is not None and credit_idx is not None:
                        debit_val = _parse_amount(cell(debit_idx) or "0")
                        credit_val = _parse_amount(cell(credit_idx) or "0")
                        if debit_val > 0:
                            monto, tipo = debit_val, TransactionType.debit
                        elif credit_val > 0:
                            monto, tipo = credit_val, TransactionType.credit
                        else:
                            continue
                    elif amount_idx is not None:
                        monto = _parse_amount(cell(amount_idx) or "0")
                        tipo = TransactionType.credit if monto >= 0 else TransactionType.debit
                        monto = abs(monto)
                    else:
                        continue

                    transactions.append(
                        Transaction(fecha=fecha, descripcion=descripcion, monto=monto, tipo=tipo)
                    )

    return transactions
