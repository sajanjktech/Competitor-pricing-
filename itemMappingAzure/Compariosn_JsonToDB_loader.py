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

# Folder containing final matching output JSONs
MATCH_FOLDER = os.path.join(os.path.dirname(__file__), "../JsonOutput_Ryan_jet2_eurowings")

json_files = [
    f for f in os.listdir(MATCH_FOLDER)
    if f.lower().endswith(".json")
]

print(f"📁 Found {len(json_files)} match JSON files.\n")

# ---------------------------------------------------------
# SQL INSERT (updated to match FINAL TABLE DDL)
# ---------------------------------------------------------
insert_sql = text("""
INSERT INTO dbo.competitor_price_comparison (
    gate_item_row_id,
    gate_item_name,
    competitor_item_id,
    competitor_name,
    competitor_item_name,
    gate_item_description,
    gate_parent_category,
    gate_sales_category,
    competitor_item_description,
    competitor_parent_category,
    competitor_sales_category,
    competitor_item_price,
    competitor_item_currency,
    competitor_item_brand,
    competitor_item_quantity,
    competitor_catalog_name,
    competitor_catalog_start,
    competitor_catalog_end,
    competitor_catalog_page,
    similarity_name,
    similarity_description,
    similarity_parent_category,
    similarity_sales_category,
    similarity_final_score
)
VALUES (
    :gate_item_row_id,
    :gate_item_name,
    :competitor_item_id,
    :competitor_name,
    :competitor_item_name,
    :gate_item_description,
    :gate_parent_category,
    :gate_sales_category,
    :competitor_item_description,
    :competitor_parent_category,
    :competitor_sales_category,
    :competitor_item_price,
    :competitor_item_currency,
    :competitor_item_brand,
    :competitor_item_quantity,
    :competitor_catalog_name,
    :competitor_catalog_start,
    :competitor_catalog_end,
    :competitor_catalog_page,
    :similarity_name,
    :similarity_description,
    :similarity_parent_category,
    :similarity_sales_category,
    :similarity_final_score
)
""")

# ---------------------------------------------------------
# PROCESS FILES
# ---------------------------------------------------------
total_inserted = 0

for jf in json_files:
    print(f"➡ Processing {jf}")
    path = os.path.join(MATCH_FOLDER, jf)

    with open(path, "r", encoding="utf-8") as f:
        blocks = json.load(f)

    with engine.begin() as conn:
        for block in blocks:

            gg = block["gate_item"]

            for m in block["matches"]:

                row = {
                    "gate_item_row_id": gg["id"],
                    "gate_item_name": gg["name"],
                    "gate_item_description": gg["desc"],
                    "gate_parent_category": gg["parent_category"],
                    "gate_sales_category": gg["sales_category"],

                    "competitor_item_id": m["competitor_item_id"],
                    "competitor_item_name": m["competitor_item_name"],
                    "competitor_item_brand": m.get("brand"),
                    "competitor_item_quantity": m.get("quantity"),
                    "competitor_item_description": m.get("competitor_description"),
                    "competitor_parent_category": m.get("parent_category"),
                    "competitor_sales_category": m.get("sales_category"),

                    # NEW FIELDS (price + currency)
                    "competitor_item_price": m.get("price"),
                    "competitor_item_currency": m.get("currency"),

                    # Catalog metadata
                    "competitor_name": m.get("competitor_name"),
                    "competitor_catalog_name": m.get("catalog_name"),
                    "competitor_catalog_start": m.get("catalog_start"),
                    "competitor_catalog_end": m.get("catalog_end"),
                    "competitor_catalog_page": m.get("competitor_page"),

                    # Similarity scores
                    "similarity_name": m["similarity_name"],
                    "similarity_description": m["similarity_desc"],
                    "similarity_parent_category": m["similarity_parent"],
                    "similarity_sales_category": m["similarity_sales"],
                    "similarity_final_score": m["similarity_final"]
                }

                conn.execute(insert_sql, row)
                total_inserted += 1

print(f"\n✅ Total Rows Inserted: {total_inserted}")
