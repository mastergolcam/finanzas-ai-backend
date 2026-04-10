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
DEBIT_ALIASES = {"debito", "débito", "cargo", "egreso", "salida", "debit", "valor débito", "valor debito"}
CREDIT_ALIASES = {"credito", "crédito", "abono", "ingreso", "entrada", "credit", "valor crédito", "valor credito"}
TYPE_ALIASES = {"tipo", "type", "movimiento", "operacion", "operación"}

PAGO_TC_KEYWORDS = ("pago a la tarjeta", "gracias por su pago")

# Global66: rows to ignore entirely
GLOBAL66_IGNORE = ("intereses abonados", "conversión de divisas", "debito sin descripcion")

# Global66: rows to include as "Impuestos / Comisiones Bancarias"
GLOBAL66_COMISION_KEYWORDS = ("gmf", "4x1.000", "comisión", "comision")

# Nu: rows to ignore
NU_IGNORE_KEYWORDS = ("gracias por tu pago",)

# Nu: Spanish month abbreviations
NU_MONTHS = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}


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


# ---------------------------------------------------------------------------
# XLS parser
# ---------------------------------------------------------------------------

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
            debit_val = abs(_parse_amount(row[debit_col]))
            credit_val = abs(_parse_amount(row[credit_col]))
            if debit_val > 0:
                monto = debit_val
                tipo = TransactionType.debit
            elif credit_val > 0:
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

        # Pagos a tarjeta de crédito → crédito con categoría fija
        categoria = None
        if any(kw in descripcion.lower() for kw in PAGO_TC_KEYWORDS):
            tipo = TransactionType.credit
            categoria = "Pagos TC"

        transactions.append(
            Transaction(fecha=fecha, descripcion=descripcion, monto=monto, tipo=tipo, categoria=categoria)
        )

    return transactions


# ---------------------------------------------------------------------------
# PDF — bank detection
# ---------------------------------------------------------------------------

def _detect_bank(full_text: str) -> str:
    text_lower = full_text.lower()
    if "global66" in text_lower or "global 66" in text_lower:
        return "global66"
    if "nu colombia" in text_lower or "nucolombia" in text_lower or "nu bank" in text_lower:
        return "nu"
    return "generic"


# ---------------------------------------------------------------------------
# PDF — Global66
# ---------------------------------------------------------------------------

# Pattern: "2026-01-05 13:02:14  Compra en Homecenter chia  9671698  7865  $249,900  $9,236,492"
# Groups:  date+time | description | mov_num | card_num | debit_or_abono | saldo
# The debit/abono column may be missing (only one monetary value before saldo).
# We detect debit vs credit by checking which monetary column has a value.
_GLOBAL66_ROW = re.compile(
    r"(\d{4}-\d{2}-\d{2})\s+\d{2}:\d{2}:\d{2}\s+"   # fecha datetime
    r"(.+?)\s+"                                         # descripción
    r"(\d{6,})\s+"                                      # número movimiento
    r"(\d{3,})\s+"                                      # número tarjeta (parcial)
    r"(\$[\d.,]+)\s+"                                   # débito o abono
    r"(\$[\d.,]+)"                                      # saldo
)

def _parse_global66(full_text: str) -> List[Transaction]:
    transactions = []

    # Extract header line to determine column order (Débito / Abono)
    # We look for the header row that contains both words
    header_debit_first = True  # default: Débito comes before Abono
    for line in full_text.splitlines():
        ll = line.lower()
        if "débito" in ll and "abono" in ll:
            header_debit_first = ll.index("débito") < ll.index("abono")
            break

    for line in full_text.splitlines():
        line = line.strip()
        if not line:
            continue

        m = _GLOBAL66_ROW.search(line)
        if not m:
            continue

        fecha_str, descripcion, _, _, amount_col_val, _ = m.groups()
        descripcion = descripcion.strip()

        desc_lower = descripcion.lower()

        # Skip rows ignored entirely
        if any(ign in desc_lower for ign in GLOBAL66_IGNORE):
            continue

        fecha = _parse_date(fecha_str)
        monto = abs(_parse_amount(amount_col_val))
        if monto == 0:
            continue

        # GMF / comisiones → debit con categoría fija
        if any(kw in desc_lower for kw in GLOBAL66_COMISION_KEYWORDS):
            descripcion = "GMF 4x1000" if "gmf" in desc_lower or "4x1.000" in desc_lower else descripcion
            transactions.append(
                Transaction(
                    fecha=fecha,
                    descripcion=descripcion,
                    monto=monto,
                    tipo=TransactionType.debit,
                    categoria="Impuestos / Comisiones Bancarias",
                )
            )
            continue

        # Infer debit/credit from description keywords
        abono_keywords = ("abono", "transferencia recibida", "pago recibido", "recarga")
        tipo = (
            TransactionType.credit
            if any(kw in desc_lower for kw in abono_keywords)
            else TransactionType.debit
        )

        categoria = None
        if any(kw in desc_lower for kw in PAGO_TC_KEYWORDS):
            tipo = TransactionType.credit
            categoria = "Pagos TC"

        transactions.append(
            Transaction(fecha=fecha, descripcion=descripcion, monto=monto, tipo=tipo, categoria=categoria)
        )

    return transactions


# ---------------------------------------------------------------------------
# PDF — Nu
# ---------------------------------------------------------------------------

# Nu date format: "05 ene 2026"
_NU_DATE = re.compile(r"(\d{1,2})\s+([a-záéíóú]{3})\s+(\d{4})", re.IGNORECASE)

# Nu cuotas: "1 de 3", "1 de 1"  — we only want "1 de X"
_NU_CUOTAS = re.compile(r"(\d+)\s+de\s+(\d+)", re.IGNORECASE)

# Nu amount: "$249.900" or "-$249.900"
_NU_AMOUNT = re.compile(r"-?\$[\d.,]+")


def _parse_nu_date(text: str) -> Optional[date]:
    m = _NU_DATE.search(text)
    if not m:
        return None
    day, month_str, year = m.groups()
    month = NU_MONTHS.get(month_str.lower())
    if not month:
        return None
    try:
        return date(int(year), month, int(day))
    except ValueError:
        return None


def _parse_nu(full_text: str) -> List[Transaction]:
    transactions = []
    lines = [l.strip() for l in full_text.splitlines() if l.strip()]

    i = 0
    while i < len(lines):
        line = lines[i]

        # A transaction line starts with a date
        fecha = _parse_nu_date(line)
        if fecha is None:
            i += 1
            continue

        # Collect continuation lines until next date or end
        block_lines = [line]
        j = i + 1
        while j < len(lines) and _parse_nu_date(lines[j]) is None:
            block_lines.append(lines[j])
            j += 1
        i = j

        block = " ".join(block_lines)
        block_lower = block.lower()

        # Skip payment rows
        if any(kw in block_lower for kw in NU_IGNORE_KEYWORDS):
            continue

        # Find cuotas — only include "1 de X"
        cuotas_match = _NU_CUOTAS.search(block)
        if cuotas_match:
            current_cuota = int(cuotas_match.group(1))
            if current_cuota != 1:
                continue

        # Extract amount
        amounts = _NU_AMOUNT.findall(block)
        if not amounts:
            continue
        raw_amount = amounts[0]
        is_negative = raw_amount.startswith("-")
        monto = abs(_parse_amount(raw_amount))
        if monto == 0:
            continue

        # Extract description: text between date portion and first amount
        desc_match = re.search(
            r"\d{1,2}\s+[a-záéíóú]{3}\s+\d{4}\s+(.+?)(?=\s+-?\$)", block, re.IGNORECASE
        )
        descripcion = desc_match.group(1).strip() if desc_match else block[:80].strip()

        # Nu card statements are debits; negative values are credits/refunds
        tipo = TransactionType.credit if is_negative else TransactionType.debit

        categoria = None
        if any(kw in block_lower for kw in PAGO_TC_KEYWORDS):
            tipo = TransactionType.credit
            categoria = "Pagos TC"

        transactions.append(
            Transaction(fecha=fecha, descripcion=descripcion, monto=monto, tipo=tipo, categoria=categoria)
        )

    return transactions


# ---------------------------------------------------------------------------
# PDF — Generic (table-based fallback)
# ---------------------------------------------------------------------------

def _parse_generic_pdf(file_bytes: bytes) -> List[Transaction]:
    transactions = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2:
                    continue

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
                        debit_val = abs(_parse_amount(cell(debit_idx) or "0"))
                        credit_val = abs(_parse_amount(cell(credit_idx) or "0"))
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


# ---------------------------------------------------------------------------
# Public PDF entry point
# ---------------------------------------------------------------------------

PDF_DEBUG_PATH = "/tmp/pdf_debug.txt"


def parse_pdf(file_bytes: bytes) -> List[Transaction]:
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        full_text = "\n".join(
            page.extract_text() or "" for page in pdf.pages
        )

    # Write first 50 lines to debug file
    lines = full_text.splitlines()
    debug_content = "\n".join(lines[:50])
    with open(PDF_DEBUG_PATH, "w", encoding="utf-8") as f:
        f.write(debug_content)

    bank = _detect_bank(full_text)

    if bank == "global66":
        return _parse_global66(full_text)
    if bank == "nu":
        return _parse_nu(full_text)
    return _parse_generic_pdf(file_bytes)
