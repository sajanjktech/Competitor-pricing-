# embedder.py
from itemMapping.logger import logger
from itemMapping.config import get_openai_client
import numpy as np

client = get_openai_client()
EMBED_MODEL = "text-embedding-3-large"

def embed_text(text):
    try:
        logger.info(f"Embedding text: {text[:40]}...")
        resp = client.embeddings.create(model=EMBED_MODEL, input=text)
        logger.info("Embedding successful.")
        return resp.data[0].embedding
    except Exception as e:
        logger.error(f"Embedding FAILED for text: {text[:60]}")
        logger.error(e)
        return None


def cosine(a, b):
    try:
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
    except Exception as e:
        logger.error("Cosine similarity FAILED")
        logger.error(e)
        return 0.0