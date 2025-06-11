# etl/risk_score/risk_api.py
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
import random

load_dotenv()  # if you want to read config in future

app = FastAPI(title="Risk Score Service")

class RiskRequest(BaseModel):
    customer_id: int

@app.post("/risk_score")
async def get_risk(req: RiskRequest):
    if req.customer_id < 1:
        raise HTTPException(400, "Invalid customer_id")
    # Simple pseudo‐risk: uniform [0.0,1.0)
    score = round((req.customer_id % 100) / 100 + random.uniform(-0.1,0.1), 2)
    score = max(0.0, min(1.0, score))  # clamp [0,1]
    return {"customer_id": req.customer_id, "risk_score": score}
