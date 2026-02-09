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
MATCH_FOLDER = os.path.join(os.path.dirname(__file__))

json_files = [
    f for f in os.listdir(MATCH_FOLDER)
    if f.lower().endswith(".json")
]

print(f"📁 Found {len(json_files)} match JSON files.\n")


# ---------------------------------------------------------
# SQL INSERT (updated to match FINAL TABLE DDL)
# ---------------------------------------------------------
insert_sql = text("""
INSERT INTO dbo.llm_competitor_price_comparison (
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
    similarity_score,
    reasoning,
    tags
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
    :similarity_score,
    :reasoning,
    :tags
)
""")


# ---------------------------------------------------------
# SAFE MATCH VALIDATION
# ---------------------------------------------------------
def is_valid_match(m):
    """Skip empty, malformed, or incomplete match entries."""
    if not isinstance(m, dict):
        return False

    # Required minimal fields
    required = ["competitor_item_id", "competitor_item_name", "similarity_final"]

    for key in required:
        if key not in m or m[key] in (None, "", []):
            return False

    return True


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

            for m in block.get("matches", []):

                # ❗ SKIP invalid match items
                if not is_valid_match(m):
                    continue

                row = {
                    "gate_item_row_id": gg["id"],
                    "gate_item_name": gg["name"],
                    "gate_item_description": gg.get("desc"),
                    "gate_parent_category": gg.get("parent_category"),
                    "gate_sales_category": gg.get("sales_category"),

                    "competitor_item_id": m["competitor_item_id"],
                    "competitor_item_name": m["competitor_item_name"],
                    "competitor_item_brand": m.get("brand"),
                    "competitor_item_quantity": m.get("quantity"),
                    "competitor_item_description": m.get("competitor_description"),
                    "competitor_parent_category": m.get("parent_category"),
                    "competitor_sales_category": m.get("sales_category"),

                    "competitor_item_price": m.get("price"),
                    "competitor_item_currency": m.get("currency"),

                    "competitor_name": m.get("competitor_name"),
                    "competitor_catalog_name": m.get("catalog_name"),
                    "competitor_catalog_start": m.get("catalog_start"),
                    "competitor_catalog_end": m.get("catalog_end"),
                    "competitor_catalog_page": m.get("competitor_page"),

                
                    "similarity_score": m["similarity_final"],
                    "reasoning": m.get("reasoning"),
                    "tags": m.get("tags")
                }

                conn.execute(insert_sql, row)
                total_inserted += 1

print(f"\n✅ Total Rows Inserted: {total_inserted}")
