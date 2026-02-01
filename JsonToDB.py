import json
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------
# SQL CONNECTION USING pymssql (NO ODBC REQUIRED)
# ---------------------------------------------------------
server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")

connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}"
engine = create_engine(connection_string)

# ---------------------------------------------------------
# INPUT JSON FILE
# ---------------------------------------------------------
json_file = "output/Ryanair LLM_PRO.json"

with open(json_file, "r", encoding="utf-8") as f:
    items = json.load(f)

print(f"Loaded {len(items)} items from JSON\n")

# ---------------------------------------------------------
# KEY MAPPING: incoming JSON → DB schema
# ---------------------------------------------------------
def map_keys(raw):
    return {
        "Competitor_Name": raw.get("competitor_name", ""),
        "Catalog_effective_start_date": raw.get("catalog_start"),
        "Catalog_effective_end_date": raw.get("catalog_end"),
        "Item_name": raw.get("item_name"),
        "Item_description": raw.get("description"),
        "Item_brand": raw.get("brand", ""),
        "Item_Quantity": raw.get("quantity", ""),
        "Item_parent_category": raw.get("parent_category", ""),
        "Item_sales_category": raw.get("sales_category", ""),
        "Item_price": raw.get("price"),
        "Item_currency": raw.get("currency"),
        "menu_page": str(raw.get("page", "")),
    }

# ---------------------------------------------------------
# SQL INSERT STATEMENT
# ---------------------------------------------------------
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

# ---------------------------------------------------------
# VALIDATION RULES
# ---------------------------------------------------------
def is_invalid(mapped):
    return (
        not mapped["Item_name"] or
        mapped["Item_price"] in [None, "", "null"]
    )

# ---------------------------------------------------------
# PROCESS & INSERT
# ---------------------------------------------------------
count_inserted = 0
count_skipped = 0
skipped_rows = []

with engine.begin() as conn:
    for raw in items:
        mapped = map_keys(raw)

        # Apply skip rules
        if is_invalid(mapped):
            count_skipped += 1
            skipped_rows.append(raw)
            continue

        conn.execute(insert_sql, mapped)
        count_inserted += 1

# ---------------------------------------------------------
# REPORT
# ---------------------------------------------------------
print("-------------------------------------------------")
print(f"Inserted rows : {count_inserted}")
print(f"Skipped rows  : {count_skipped}")
print("-------------------------------------------------")

if count_skipped > 0:
    print("\n❗ Showing first 3 skipped rows for debugging:\n")
    for r in skipped_rows[:3]:
        print(json.dumps(r, indent=2))
