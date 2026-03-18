# app/api.py

from fastapi import FastAPI
from pydantic import BaseModel

from semantic.orchestrator import SemanticOrchestrator
from app_entry import run_query

app = FastAPI(
    title="Wave Sales Semantic API",
    version="1.0.0"
)


class QueryRequest(BaseModel):
    question: str


@app.post("/run")
def run(req: QueryRequest):
    print(f"API Received question: {req.question}", flush=True)
    try:
        res = run_query(req.question)
        print("API Question executed successfully", flush=True)
        return res
    except Exception as e:
        print(f"API Error: {e}", flush=True)
        raise e
