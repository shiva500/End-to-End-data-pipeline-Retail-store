# etl/orders/generate_orders.py
from dotenv import load_dotenv
import os
import io
import boto3
import pandas as pd
from faker import Faker
from datetime import datetime


# 1. Load AWS and bucket settings from root .env
load_dotenv()  
BUCKET = os.getenv("ORDERS_BUCKET")
PREFIX = os.getenv("ORDERS_PREFIX", "orders")
print("▶ All env keys:", [k.values() for k in os.environ if k.startswith("ORDERS_")])

# 2. Initialize Faker and S3 client
fake = Faker()
s3   = boto3.client("s3")

def make_orders_dataframe(n=500):
    """Create a Pandas DataFrame of n fake order records,
       now including location and product description."""
    rows = []
    for _ in range(n):
        rows.append({
            "order_id":             fake.unique.random_int(1, 1_000_000),
            "order_timestamp":      fake.date_time_between("-1d", "now"),
            "customer_id":          fake.random_int(1, 1_000),
            "product_id":           fake.random_int(1, 500),
            "quantity":             fake.random_int(1, 10),
            "unit_price":           round(fake.random_number(4) / 100, 2),
            "location":             fake.city(),                       # New: city name
            "product_description":  fake.sentence(nb_words=4)          # New: short product blurb
        })
    return pd.DataFrame(rows)

def upload_df_to_s3(df):
    """Serialize the DataFrame to CSV in memory and upload to S3."""
    timestamp  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key        = f"{PREFIX}/orders_{timestamp}.csv"
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=BUCKET, Key=key, Body=csv_buffer.getvalue())
    print(f"✔ Uploaded {len(df)} orders to s3://{BUCKET}/{key}")

if __name__ == "__main__":
    # Generate & upload a new batch of fake orders
    df = make_orders_dataframe(n=1000)  
    upload_df_to_s3(df)
