# test_pytds_sql.py

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")

# --- USE PYTDS DRIVER ---
conn_str = f"mssql+pytds://{username}:{password}@{server}/{database}"

print("\n🔵 Testing Azure SQL connection using pytds...\n")

try:
    engine = create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_recycle=180,
        connect_args={"timeout": 30}
    )

    with engine.connect() as conn:
        print("⚡ Running test query: SELECT COUNT(*) FROM item ...")
        result = conn.execute(text("SELECT COUNT(*) FROM item"))
        row = result.fetchone()
        print("\n✅ SQL connection SUCCESS!")
        print(f"Total rows in item table: {row[0]}\n")

except Exception as e:
    print("❌ SQL connection FAILED!")
    print(e)