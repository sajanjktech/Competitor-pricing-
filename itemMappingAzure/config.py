# config.py
from itemMappingAzure.logger import logger
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from openai import AzureOpenAI

load_dotenv()


def get_openai_client():
    """Return Azure OpenAI client."""
    try:
        logger.info("Initializing Azure OpenAI client...")
        client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version="2024-02-15-preview",
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        logger.info("Azure OpenAI client initialized.")
        return client
    except Exception as e:
        logger.error(f"OpenAI client init FAILED: {e}")
        raise


def get_sql_engine():
    """Return SQLAlchemy engine (Azure SQL + pymssql)."""
    try:
        logger.info("Connecting to Azure SQL...")

        server = os.getenv("AZURE_SQL_SERVER")
        database = os.getenv("AZURE_SQL_DATABASE")
        username = os.getenv("AZURE_SQL_USERNAME")
        password = os.getenv("AZURE_SQL_PASSWORD")

        conn_str = f"mssql+pymssql://{username}:{password}@{server}/{database}"
        engine = create_engine(conn_str)

        # IMPORTANT: wrap SQL in text()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.info("Connected to Azure SQL successfully.")
        return engine

    except Exception as e:
        logger.error("Azure SQL connection FAILED!")
        logger.error(e, exc_info=True)
        raise