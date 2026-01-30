import os
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")

# Use same working connection method
connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}"
engine = create_engine(connection_string)

print("🔵 Fetching GateGroup items...")

sql = text("""
    SELECT DISTINCT
        i.item_row_id,
        i.item_name,
        i.item_description,
        p.item_price,
        p.item_currency_code,
        p.item_price_effective_start,
        p.item_price_effective_end
    FROM item i
    JOIN item_price p ON i.item_row_id = p.item_row_id
    WHERE 
        p.item_currency_code = 'GBP'
        AND i.item_sales_category_name IN (
            'Alcohol', 'Cold Drink', 'Fresh Food', 'Hot Drink',
            'Savoury Snacks', 'Sweet Snacks',
            'Gents Fragrance', 'Ladies Fragrance', 'DF Spirits'
        )
        AND p.item_price_effective_start >= '2025-01-01'
        AND p.item_price_effective_end   <= '2025-04-30'
""")

try:
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

        print(f"✅ Total GateGroup items fetched: {len(rows)}")

        # Convert to JSON serializable list
        items = []
        for r in rows:
            items.append({
                "item_row_id": r.item_row_id,
                "item_name": r.item_name,
                "item_description": r.item_description,
                "item_price": float(r.item_price) if r.item_price is not None else None,
                "item_currency_code": r.item_currency_code,
                "item_price_effective_start": str(r.item_price_effective_start),
                "item_price_effective_end": str(r.item_price_effective_end)
            })

        # Save JSON
        with open("gate_group_items.json", "w", encoding="utf-8") as f:
            json.dump(items, f, indent=2, ensure_ascii=False)

        print("💾 Saved → gate_group_items.json")

except Exception as e:
    print("❌ SQL Fetch Failed")
    print(e)