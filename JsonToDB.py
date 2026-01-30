import json
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")

# TDS-based connection (NO ODBC)
connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}"

engine = create_engine(connection_string)

json_file = "output/Jet2.pdf_combined.json"

with open(json_file, "r", encoding="utf-8") as f:
    items = json.load(f)

insert_sql = text("""
INSERT INTO dbo.competitor_item_details (
    Competitor_Name,
    Catalog_effective_start_date,
    Catalog_effective_end_date,
    Item_name,
    Item_description,
    Item_brand,
    Item_Quantity,
    Item_parent_category,
    Item_sales_category,
    Item_price,
    Item_currency,
    menu_page
)
VALUES (
    :Competitor_Name,
    :Catalog_effective_start_date,
    :Catalog_effective_end_date,
    :Item_name,
    :Item_description,
    :Item_brand,
    :Item_Quantity,
    :Item_parent_category,
    :Item_sales_category,
    :Item_price,
    :Item_currency,
    :menu_page
)
""")

count_inserted = 0
count_skipped = 0

with engine.begin() as conn:
    for item in items:

        # Skip if Item_name is None, empty, or missing
        if not item.get("Item_name"):
            count_skipped += 1
            continue

        conn.execute(insert_sql, item)
        count_inserted += 1

print("Inserted:", count_inserted)
print("Skipped:", count_skipped)
