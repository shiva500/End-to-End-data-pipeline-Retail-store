# etl/orders/load_orders.py

import os
import io
import boto3
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

BUCKET       = os.getenv("ORDERS_BUCKET")
PREFIX       = os.getenv("ORDERS_PREFIX", "orders")
DB_URL       = os.getenv("DATABASE_URL")
RAW_SCHEMA   = os.getenv("RAW_SCHEMA", "raw_orders")
ORDERS_TABLE = os.getenv("ORDERS_TABLE", "orders")

def list_s3_keys(s3):
    """Yield all object keys under the given prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def copy_csv_to_postgres(cur, s3, key):
    """Download a CSV from S3 and COPY it into Postgres."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    csv_body = obj["Body"].read().decode("utf-8")
    buf = io.StringIO(csv_body)

    copy_sql = sql.SQL(
        "COPY {}.{} FROM STDIN WITH CSV HEADER"
    ).format(
        sql.Identifier(RAW_SCHEMA),
        sql.Identifier(ORDERS_TABLE),
    )
    cur.copy_expert(copy_sql, buf)

def main():
    print(f"▶ Starting load_orders.py")
    print(f"  • S3 Bucket/Prefix: {BUCKET}/{PREFIX}")
    print(f"  • Target table:     {RAW_SCHEMA}.{ORDERS_TABLE}")
    print(f"  • Connecting to:    {DB_URL}\n")

    # Initialize clients & DB connection
    s3   = boto3.client("s3")
    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()

    # Ensure the load history table exists
    cur.execute(sql.SQL("""
        CREATE SCHEMA IF NOT EXISTS {};
        CREATE TABLE IF NOT EXISTS {}._load_history (
            s3_key     TEXT PRIMARY KEY,
            loaded_at  TIMESTAMPTZ DEFAULT now()
        );
    """).format(
        sql.Identifier(RAW_SCHEMA),
        sql.Identifier(RAW_SCHEMA),
    ))
    conn.commit()

    # Fetch already-processed keys
    cur.execute(
        sql.SQL("SELECT s3_key FROM {}._load_history").format(
            sql.Identifier(RAW_SCHEMA)
        )
    )
    processed = {row[0] for row in cur.fetchall()}

    # List all objects in S3
    keys = list(list_s3_keys(s3))
    print(f"▶ Found {len(keys)} S3 object(s).")
    loaded_count = 0

    for key in keys:
        # Skip "folder" placeholders and non-CSV files
        if key.endswith("/") or not key.lower().endswith(".csv"):
            print(f"Ignoring S3 key: {key}")
            continue
        # Skip files already loaded
        if key in processed:
            print(f"Skipping already loaded: {key}")
            continue

        # Load the new file
        print(f"  → Loading: {key} ...", end="", flush=True)
        try:
            copy_csv_to_postgres(cur, s3, key)
            cur.execute(
                sql.SQL("INSERT INTO {}._load_history (s3_key) VALUES (%s)").format(
                    sql.Identifier(RAW_SCHEMA)
                ),
                (key,)
            )
            conn.commit()
            loaded_count += 1
            print(" ✔ Success")
        except Exception as e:
            conn.rollback()
            print(f" ✗ Failed ({e.__class__.__name__})")

    # Clean up
    cur.close()
    conn.close()

    # Final summary
    print(f"\n▶ Load complete: {loaded_count} new file(s) ingested out of {len(keys)} found.")
    if loaded_count == 0:
        print("  (No new files to process.)")

if __name__ == "__main__":
    main()
