# etl/orders/load_orders_snowflake.py
from pathlib import Path
from dotenv import load_dotenv
import os
import logging
import boto3
import snowflake.connector

# ————————————————
# 1) Load your .env
# ————————————————
env_path = Path(__file__).resolve().parent.parent.parent / ".env"
print("Loading .env from:", env_path)

# strip BOM if present, show parse errors, overwrite any live vars
load_dotenv(dotenv_path=env_path, verbose=True, override=True, encoding="utf-8-sig")

# quick debug: make sure the eight SNOWFLAKE_ keys are in your environment
print("▶ Loaded env keys:", sorted(k for k in os.environ if k.startswith("SNOWFLAKE_")))

# ————————————————
# 2) Configure logging
# ————————————————
LOG_FILE = os.getenv("LOAD_LOG_FILE", "load_orders.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ————————————————
# 3) Read config
# ————————————————
BUCKET           = os.getenv("ORDERS_BUCKET")
PREFIX           = os.getenv("ORDERS_PREFIX", "orders")
STAGE_NAME       = os.getenv("SNOWFLAKE_STAGE", "my_s3_stage")
PROCESSED_PREFIX = os.getenv("ORDERS_PROCESSED_PREFIX", f"{PREFIX}/processed")

SNOW_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOW_USER      = os.getenv("SNOWFLAKE_USER")
SNOW_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOW_ROLE      = os.getenv("SNOWFLAKE_ROLE")
SNOW_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOW_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SNOW_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA")

# boto3 S3 client
s3 = boto3.client("s3")


def load_orders_to_snowflake():
    """Load new order CSVs from S3 into Snowflake and archive processed files."""
    loaded_any = False

    # ————————————————
    # 4) Connect to Snowflake
    # ————————————————
    try:
        ctx = snowflake.connector.connect(
            user      = SNOW_USER,
            password  = SNOW_PASSWORD,
            account   = SNOW_ACCOUNT,
            role      = SNOW_ROLE,
            warehouse = SNOW_WAREHOUSE,
            database  = SNOW_DATABASE,
            schema    = SNOW_SCHEMA,
        )
        cs = ctx.cursor()
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        print("❌ ETL failed: Could not connect to Snowflake.")
        return

    try:
        # ————————————————
        # 5) Ensure the target table exists
        # ————————————————
        cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {SNOW_SCHEMA}.orders (
                order_id            NUMBER,
                order_timestamp     TIMESTAMP,
                customer_id         NUMBER,
                product_id          NUMBER,
                quantity            NUMBER,
                unit_price          FLOAT,
                location            STRING,
                product_description STRING
            );
        """)
        logger.info(f"Ensured table {SNOW_SCHEMA}.orders exists.")

        # ————————————————
        # 6) List & load new CSVs from S3
        # ————————————————
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{PREFIX}/")
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".csv"):
                continue

            file_name  = key.split(f"{PREFIX}/", 1)[-1]
            stage_path = f"@{STAGE_NAME}/{file_name}"

            try:
                cs.execute(f"""
                    COPY INTO {SNOW_SCHEMA}.orders
                    FROM {stage_path}
                    FILE_FORMAT = (
                      TYPE           = 'CSV',
                      FIELD_DELIMITER= ',',
                      SKIP_HEADER    = 1
                    )
                    ON_ERROR = 'CONTINUE';
                """)
                logger.info(f"Loaded {stage_path} into Snowflake.")
                loaded_any = True

                # archive the file
                dest_key = f"{PROCESSED_PREFIX}/{file_name}"
                s3.copy_object(
                    Bucket     = BUCKET,
                    CopySource = {'Bucket': BUCKET, 'Key': key},
                    Key        = dest_key
                )
                s3.delete_object(Bucket=BUCKET, Key=key)
                logger.info(f"Archived {key} → {dest_key}.")

            except Exception as e:
                logger.error(f"Error loading {stage_path}: {e}")
                print(f"❌ Failed loading {stage_path}: {e}")

        # ————————————————
        # 7) Final status
        # ————————————————
        if loaded_any:
            logger.info("ETL completed successfully: data loaded & archived.")
            print("✅ ETL completed: New data loaded and archived.")
        else:
            logger.info("ETL completed: no new CSVs to load.")
            print("⚠️ ETL completed: No new files found.")

    finally:
        cs.close()
        ctx.close()


if __name__ == "__main__":
    load_orders_to_snowflake()
