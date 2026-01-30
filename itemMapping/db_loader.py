# db_loader.py
from itemMapping.logger import logger
from itemMapping.config import get_sql_engine
from sqlalchemy import text

engine = get_sql_engine()


def load_gate_group_items():
    """Load GateGroup items from SQL Server."""
    logger.info("Loading GateGroup items...")

    sql = text("""
SELECT
    i.item_row_id,
    i.item_onboard_name,
    i.item_description,
    CONCAT(
        MIN(p.item_price), 
        ' - ', 
        MAX(p.item_price)
    ) AS item_price,
    p.item_currency_code
FROM item i
JOIN item_price p 
    ON i.item_row_id = p.item_row_id
WHERE 
    p.item_currency_code = 'GBP'
    AND i.item_sales_category_name IN (
        'Alcohol','Cold Drink','Fresh Food','Hot Drink',
        'Savoury Snacks','Sweet Snacks',
        'Gents Fragrance','Ladies Fragrance','DF Spirits'
    )
GROUP BY
    i.item_row_id,
    i.item_onboard_name,
    i.item_description,
    p.item_currency_code;
            
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
            logger.info(f"GateGroup items loaded: {len(rows)}")
            return rows
    except Exception as e:
        logger.error("Failed loading GateGroup items")
        logger.error(e, exc_info=True)
        raise


def load_competitor_items():
    """Load competitor items from SQL Server."""
    logger.info("Loading competitor items...")

    sql = text("""
        SELECT 
            id, Item_name, Item_description, Item_price, Item_currency
        FROM dbo.competitor_item_details;
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
            logger.info(f"Competitor items loaded: {len(rows)}")
            return rows
    except Exception as e:
        logger.error("Failed loading competitor items")
        logger.error(e, exc_info=True)
        raise