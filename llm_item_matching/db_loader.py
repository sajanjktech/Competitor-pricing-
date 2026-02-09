# db_loader.py

from sqlalchemy import text
from config import get_sql_engine

engine = get_sql_engine()


def load_gate_group_items():
    """Load GG items only for Alcohol and DF Spirits."""
    sql = text("""
        SELECT 
            item_row_id,
            item_name,
            item_description,
            item_parent_sales_category_name,
            item_sales_category_name
        FROM item
        WHERE item_sales_category_name IN ('Alcohol', 'DF Spirits')
    """)
    with engine.connect() as conn:
        return conn.execute(sql).fetchall()


def load_competitor_items():
    """Load competitor items for Alcohol + DF Spirits."""
    sql = text("""
        SELECT 
            item_id,
            brand,
            item_name,
            quantity,
            item_description,
            parent_category,
            sales_category,
            price,
            currency,
            competitor_name,
            catalog_name,
            catalog_start,
            catalog_end,
            page
        FROM competitor_item_details
        WHERE sales_category IN ('Alcohol', 'DF Spirits')
    """)
    with engine.connect() as conn:
        return conn.execute(sql).fetchall()
