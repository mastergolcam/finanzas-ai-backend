from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import upload, transactions

app = FastAPI(
    title="FinanzasAI API",
    description="Backend para gestión financiera personal con categorización inteligente usando Claude AI.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload.router, tags=["Upload"])
app.include_router(transactions.router, tags=["Transactions"])


@app.get("/", tags=["Health"])
async def root():
    return {"status": "ok", "app": "FinanzasAI"}
