import snowflake.connector
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )

def extract_from_snowflake():
    conn = get_snowflake_connection()
    query = "SELECT * FROM RAW_CUSTOMERS"
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"Extracted {len(df)} rows from Snowflake")
    return df

def transform_data(df):
    # Drop duplicates
    df = df.drop_duplicates(subset=['EMAIL'])

    # Drop rows with missing name
    df = df.dropna(subset=['NAME'])

    # Fill missing age with average
    df['AGE'] = df['AGE'].fillna(df['AGE'].mean()).round()

    # Fill missing email with placeholder
    df['EMAIL'] = df['EMAIL'].fillna('unknown@email.com')

    # Add derived columns
    df['DAYS_SINCE_ORDER'] = (pd.Timestamp.now() - pd.to_datetime(df['LAST_ORDER_DATE'])).dt.days
    df['IS_CHURNED'] = df['DAYS_SINCE_ORDER'] > 90

    # Rename columns to lowercase
    df.columns = df.columns.str.lower()

    print(f"Transformed data has {len(df)} rows")
    return df

def load_to_postgres(df):
    db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    engine = create_engine(db_url)
    df.to_sql('cleaned_customers', engine, if_exists='replace', index=False)
    print(f"Loaded {len(df)} rows to PostgreSQL")

def run_pipeline():
    print("Starting ETL pipeline...")
    df = extract_from_snowflake()
    df = transform_data(df)
    load_to_postgres(df)
    print("Pipeline completed successfully!")
    return df

if __name__ == "__main__":
    run_pipeline()