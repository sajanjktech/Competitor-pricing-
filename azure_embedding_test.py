import os
from dotenv import load_dotenv
from openai import AzureOpenAI
import logging
import math

# -------------------------------------------------------
# LOGGING SETUP
# -------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="\n[%(levelname)s] %(message)s\n"
)
logger = logging.getLogger("test")


# -------------------------------------------------------
# LOAD ENV
# -------------------------------------------------------
load_dotenv()

AZURE_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_EMBED_MODEL = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")

if not (AZURE_KEY and AZURE_ENDPOINT and AZURE_EMBED_MODEL):
    logger.error("❌ Missing required environment variables")
    exit()


# -------------------------------------------------------
# INIT CLIENT
# -------------------------------------------------------
def init_client():
    try:
        logger.info("🔄 Initializing Azure OpenAI client...")
        client = AzureOpenAI(
            api_key=AZURE_KEY,
            azure_endpoint=AZURE_ENDPOINT,
            api_version="2024-02-15-preview"
        )
        logger.info("✅ Client initialized.")
        return client
    except Exception as e:
        logger.error(f"❌ Client initialization FAILED: {e}")
        exit()


client = init_client()


# -------------------------------------------------------
# NORMALIZATION
# -------------------------------------------------------
def normalize(vec):
    """L2 normalize the embedding."""
    norm = math.sqrt(sum(x*x for x in vec))
    return [x / norm for x in vec] if norm > 0 else vec


# -------------------------------------------------------
# COSINE SIMILARITY
# -------------------------------------------------------
def cosine(v1, v2):
    v1 = normalize(v1)
    v2 = normalize(v2)
    return sum(a*b for a, b in zip(v1, v2))


# -------------------------------------------------------
# CONTEXT ENRICHMENT (IMPORTANT FOR PRODUCT MATCHING)
# -------------------------------------------------------
def enrich(text):
    """
    Azure embeddings improve significantly with contextual hints.
    """
    return f"{text}"


# -------------------------------------------------------
# TEST EMBEDDING FUNCTION
# -------------------------------------------------------
def get_azure_embedding(text):
    try:
        logger.info(f"🔍 Requesting embedding for: '{text}'")
        resp = client.embeddings.create(
            model=AZURE_EMBED_MODEL,
            input=text
        )
        emb = resp.data[0].embedding
        logger.info(f"✅ Received embedding: {len(emb)} dimensions")
        return emb
    except Exception as e:
        logger.error(f"❌ Embedding FAILED: {e}")
        return None


# -------------------------------------------------------
# MAIN TEST
# -------------------------------------------------------
if __name__ == "__main__":
    logger.info("🚀 Starting Azure Embedding Test")

    # TEST VALUES
    text1 = "iPhone 15"
    text2 = "Apple smartphone latest model"

    # Apply context expansion
    enriched1 = enrich(text1)
    enriched2 = enrich(text2)

    emb1 = get_azure_embedding(enriched1)
    emb2 = get_azure_embedding(enriched2)

    if emb1 and emb2:
        sim = cosine(emb1, emb2)
        logger.info(f"\n🎯 **Cosine similarity = {round(sim, 4)}**\n")
        logger.info("🎉 Test completed successfully!")
    else:
        logger.error("❌ Test failed – one or both embeddings are None.")
