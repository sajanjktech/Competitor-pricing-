# itemMappingAzure/db_loader.py

from itemMappingAzure.logger import logger
from itemMappingAzure.config import get_sql_engine
from sqlalchemy import text

engine = get_sql_engine()

def load_gate_group_items():
    logger.info("Loading GateGroup items...")
    sql = text("""
SELECT
    i.item_row_id,
    i.item_onboard_name,
    i.item_description,
    CONCAT(MIN(p.item_price), ' - ', MAX(p.item_price)) AS item_price,
    p.item_currency_code,
    i.item_parent_sales_category_name,
    i.item_sales_category_name
FROM item i
JOIN item_price p ON i.item_row_id = p.item_row_id
WHERE p.item_currency_code = 'GBP'
  AND i.item_sales_category_name IN (
        'Alcohol','DF Spirits'
  )
GROUP BY 
    i.item_row_id,
    i.item_onboard_name,
    i.item_description,
    p.item_currency_code,
    i.item_parent_sales_category_name,
    i.item_sales_category_name;
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
        return rows


def load_competitor_items():
    logger.info("Loading competitor items...")
    sql = text("""
        SELECT id, Item_name, Item_brand, Item_description,
               Item_Quantity, Item_price, Item_currency,item_parent_category, Item_sales_category
        FROM dbo.competitor_item_details where Item_sales_category IN ('Alcohol','DF Spirits');
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
        return rows
