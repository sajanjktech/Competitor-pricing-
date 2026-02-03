import json
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------
# SQL CONNECTION
# ---------------------------------------------------------
server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")

connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}"
engine = create_engine(connection_string)

# ---------------------------------------------------------
# FOLDER CONTAINING MULTIPLE JSON FILES
# ---------------------------------------------------------
OUTPUT_FOLDER = "../llm_item_output" # Adjust the path as needed

json_files = [
    f for f in os.listdir(OUTPUT_FOLDER)
    if f.lower().endswith(".json")
]

if not json_files:
    print("❌ No JSON files found in output folder.")
    exit()

print(f"📁 Found {len(json_files)} JSON files to process.\n")

# ---------------------------------------------------------
# MAPPING FUNCTION
# ---------------------------------------------------------
def map_keys(raw):
    return {
        "competitor_name": raw.get("competitor_name"),
        "item_name": raw.get("item_name"),
        "Item_description": raw.get("description"),
        "brand": raw.get("brand"),
        "quantity": raw.get("quantity"),
        "parent_category": raw.get("parent_category"),
        "sales_category": raw.get("sales_category"),
        "price": raw.get("price"),
        "currency": raw.get("currency"),
        "catalog_name": raw.get("catalog_name"),
        "catalog_start": raw.get("catalog_start"),
        "catalog_end": raw.get("catalog_end"),
        "page": raw.get("page"),
    }

# ---------------------------------------------------------
# SQL INSERT (DO NOT INCLUDE item_id or created_at)
# ---------------------------------------------------------
insert_sql = text("""
INSERT INTO dbo.competitor_item_details (
    competitor_name,
    item_name,
    Item_description,
    brand,
    quantity,
    parent_category,
    sales_category,
    price,
    currency,
    catalog_name,
    catalog_start,
    catalog_end,
    page
)
VALUES (
    :competitor_name,
    :item_name,
    :Item_description,
    :brand,
    :quantity,
    :parent_category,
    :sales_category,
    :price,
    :currency,
    :catalog_name,
    :catalog_start,
    :catalog_end,
    :page
)
""")

# ---------------------------------------------------------
# VALIDATION: Must have item name
# ---------------------------------------------------------
def is_invalid(mapped):
    return (
        mapped["item_name"] is None or
        mapped["item_name"].strip() == "" or
        mapped["price"] is None
    )

# ---------------------------------------------------------
# PROCESS ALL JSON FILES
# ---------------------------------------------------------
total_inserted = 0
total_skipped = 0

for json_file in json_files:
    file_path = os.path.join(OUTPUT_FOLDER, json_file)
    print(f"\n📄 Processing file: {json_file}")

    with open(file_path, "r", encoding="utf-8") as f:
        items = json.load(f)

    print(f"   → Loaded {len(items)} items")

    count_inserted = 0
    count_skipped = 0

    with engine.begin() as conn:
        for raw in items:
            mapped = map_keys(raw)

            if is_invalid(mapped):
                count_skipped += 1
                continue

            conn.execute(insert_sql, mapped)
            count_inserted += 1

    print(f"   ✔ Inserted: {count_inserted}")
    print(f"   ⚠ Skipped : {count_skipped}")

    total_inserted += count_inserted
    total_skipped += count_skipped

# ---------------------------------------------------------
# FINAL REPORT
# ---------------------------------------------------------
print("\n===============================================")
print("               FINAL IMPORT REPORT             ")
print("===============================================")
print(f"Total JSON files processed: {len(json_files)}")
print(f"Total rows inserted:        {total_inserted}")
print(f"Total rows skipped:         {total_skipped}")
print("===============================================")
