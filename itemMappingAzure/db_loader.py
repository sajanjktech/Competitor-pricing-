# itemMappingAzure/db_loader.py

from itemMappingAzure.logger import logger
from itemMappingAzure.config import get_sql_engine
from sqlalchemy import text

engine = get_sql_engine()

# ---------------------------------------------------------
# LOAD GATE GROUP ITEMS  (UPDATED SQL)
# ---------------------------------------------------------
def load_gate_group_items():
    logger.info("Loading GateGroup items...")

    sql = text("""
        SELECT 
            i.item_row_id,
            i.item_onboard_name,
            i.item_description,
            i.item_parent_sales_category_name,
            i.item_sales_category_name
        FROM item i
        WHERE i.item_parent_sales_category_name IN
            ('1.Cafe','2.Boutique','3.Virtual','4.Duty Free')
        AND i.item_sales_category_name != 'logo'
        GROUP BY 
            i.item_row_id,
            i.item_onboard_name,
            i.item_description,
            i.item_parent_sales_category_name,
            i.item_sales_category_name;
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
        return rows


# ---------------------------------------------------------
# LOAD COMPETITOR ITEMS (UPDATED SQL)
# ---------------------------------------------------------
def load_competitor_items():
    logger.info("Loading competitor items...")

    sql = text("""
        SELECT 
            item_id,
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
            page,
            created_at
        FROM competitor_item_details;
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
        return rows
