import os
from typing import Optional

from fastapi import APIRouter, Form, HTTPException, Request, UploadFile, File
from fastapi.responses import PlainTextResponse

from models.schemas import UploadResponse
from services.categorizer import categorize_transactions
from services.database import save_transactions
from services.parser import parse_file, PDF_DEBUG_PATH

router = APIRouter()

ALLOWED_EXTENSIONS = {".xls", ".xlsx", ".pdf"}


@router.get("/debug-pdf", response_class=PlainTextResponse)
async def debug_pdf():
    if not os.path.exists(PDF_DEBUG_PATH):
        raise HTTPException(status_code=404, detail="No hay archivo de debug. Sube un PDF primero.")
    with open(PDF_DEBUG_PATH, "r", encoding="utf-8") as f:
        return f.read()


@router.post("/upload", response_model=UploadResponse)
async def upload_statement(
    request: Request,
    file: UploadFile = File(...),
    extracted_text: Optional[str] = Form(None),
):
    user_id = request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="user_id requerido")

    print(f"extracted_text recibido: {bool(extracted_text)}")
    print(f"primeros 200 chars: {extracted_text[:200] if extracted_text else 'NONE'}")

    filename = file.filename or ""
    ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Formato no soportado '{ext}'. Use XLS, XLSX o PDF.",
        )

    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="El archivo está vacío.")

    try:
        transactions = parse_file(file_bytes, filename, extracted_text)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Error al parsear el archivo: {str(e)}")

    if not transactions:
        raise HTTPException(
            status_code=422,
            detail="No se encontraron transacciones en el archivo. Verifique el formato.",
        )

    try:
        transactions = categorize_transactions(transactions)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error al categorizar con Claude: {str(e)}")

    try:
        save_transactions(
            transactions=transactions,
            user_id=user_id,
            source_file=filename,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error al guardar en Supabase: {str(e)}")

    return UploadResponse(total=len(transactions), transactions=transactions)
