# db_loader.py

import os
from sqlalchemy import text
from config import get_sql_engine

engine = get_sql_engine()

BASE_DIR = os.path.dirname(__file__)
DDL_DIR = os.path.join(BASE_DIR, "ddl")


def load_sql_file(filename):
    """Load SQL content from /ddl without modifying it."""
    path = os.path.join(DDL_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def execute_sql_file(filename):
    """Execute raw SQL exactly as written in the .sql file."""
    sql = load_sql_file(filename)
    with engine.connect() as conn:
        return conn.execute(text(sql)).fetchall()


def load_gate_group_items():
    """Load GG items using pure SQL from ddl/gate_items.sql."""
    return execute_sql_file("gate_items.sql")


def load_competitor_items():
    """Load competitor items using pure SQL from ddl/competitor_items.sql."""
    return execute_sql_file("competitor_items.sql")
