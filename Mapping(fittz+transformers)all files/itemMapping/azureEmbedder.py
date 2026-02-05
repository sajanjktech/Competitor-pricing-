# itemMapping/azureEmbedder.py

from itemMapping.logger import logger
from itemMapping.config import get_openai_client
import os

# Load Azure client once
client = get_openai_client()

# Azure deployment name for embeddings
AZURE_EMBED_DEPLOYMENT = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")


def get_embedding(text: str):
    """Generate embedding using Azure OpenAI embedding model."""
    if not text or text.strip() == "":
        return []

    try:
        logger.info(f"[EMB] Generating Azure embedding for text…")

        resp = client.embeddings.create(
            model=AZURE_EMBED_DEPLOYMENT,
            input=text
        )

        return resp.data[0].embedding

    except Exception as e:
        logger.error(f"Embedding FAILED: {e}")
        return []
