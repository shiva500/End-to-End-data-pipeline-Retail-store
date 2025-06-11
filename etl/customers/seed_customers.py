# etl/customers/seed_customers.py
import os
import psycopg2
from faker import Faker
from dotenv import load_dotenv

load_dotenv()  # loads CUSTOMER_DB_URL

fake = Faker()
conn = psycopg2.connect(os.getenv("CUSTOMER_DB_URL"))
cur = conn.cursor()

for cid in range(1, 1001):
    cur.execute(
        """
        INSERT INTO customer_dim (customer_id, first_name, last_name, email, signup_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
        """,
        (
            cid,
            fake.first_name(),
            fake.last_name(),
            fake.unique.email(),
            fake.date_between(start_date='-2y', end_date='today')
        )
    )

conn.commit()
cur.close()
conn.close()
print("Seeded 1,000 customer records.")
