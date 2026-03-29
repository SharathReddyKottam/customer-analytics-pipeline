from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv
from etl.pipeline import run_pipeline

load_dotenv()

app = FastAPI(title="Customer Analytics API")

def get_engine():
    db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    return create_engine(db_url)

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.post("/run-pipeline")
def trigger_pipeline():
    try:
        run_pipeline()
        return {"message": "Pipeline completed successfully!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/customers")
def get_customers():
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM cleaned_customers", engine)
    return df.to_dict(orient='records')

@app.get("/customers/churned")
def get_churned_customers():
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM cleaned_customers WHERE is_churned = TRUE", engine)
    return {"count": len(df), "customers": df.to_dict(orient='records')}

@app.get("/customers/summary")
def get_summary():
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM cleaned_customers", engine)
    summary = {
        "total_customers": len(df),
        "active_customers": int(df['is_active'].sum()),
        "churned_customers": int(df['is_churned'].sum()),
        "average_spent": round(float(df['total_spent'].mean()), 2),
        "top_city": df['city'].value_counts().index[0],
        "top_department": df['department'].value_counts().index[0]
    }
    return summary