# config.py

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from openai import AzureOpenAI

load_dotenv()


def get_sql_engine():
    """Creates SQLAlchemy engine for Azure SQL."""
    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")
    username = os.getenv("AZURE_SQL_USERNAME")
    password = os.getenv("AZURE_SQL_PASSWORD")

    conn_str = f"mssql+pymssql://{username}:{password}@{server}/{database}"
    engine = create_engine(conn_str)

    # test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    return engine


def get_llm_client():
    """Initialize Azure OpenAI client."""
    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_version="2024-02-15-preview",
    )
    return client
