# itemMappingAzure/azureEmbedder.py

import os
import json
import math
from itemMappingAzure.logger import logger
from itemMappingAzure.config import get_openai_client

# ---------------------------------------------------------
# CACHE FILES
# ---------------------------------------------------------
GG_CACHE_FILE = "gg_embeddings.json"
COMP_CACHE_FILE = "comp_embeddings.json"


def load_cache(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


gg_cache = load_cache(GG_CACHE_FILE)
comp_cache = load_cache(COMP_CACHE_FILE)


def save_gg_cache():
    with open(GG_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(gg_cache, f, indent=2)


def save_comp_cache():
    with open(COMP_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(comp_cache, f, indent=2)


# ---------------------------------------------------------
# INIT EMBEDDING CLIENT
# ---------------------------------------------------------
client = get_openai_client()
MODEL = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")


# ---------------------------------------------------------
# NORMALIZE VECTOR
# ---------------------------------------------------------
def normalize(vec):
    norm = math.sqrt(sum(x * x for x in vec))
    return [x / norm for x in vec] if norm else vec


# ---------------------------------------------------------
# EMBEDDING CREATOR (cached)
# ---------------------------------------------------------
def get_embedding(text: str, key: str, cache_type: str):
    cache = gg_cache if cache_type == "gg" else comp_cache
    save_fn = save_gg_cache if cache_type == "gg" else save_comp_cache

    if key in cache:
        return cache[key]

    try:
        logger.info(f"[EMBED] Generating embedding: {key}")

        resp = client.embeddings.create(
            model=MODEL,
            input=text
        )

        emb = normalize(resp.data[0].embedding)
        cache[key] = emb
        save_fn()
        return emb

    except Exception as e:
        logger.error(f"❌ Embedding failed for {key}: {e}")
        return []


# ---------------------------------------------------------
# ENRICHMENT HELPERS
# ---------------------------------------------------------
def enrich_name(name): return f"Product Name: {name}"
def enrich_desc(desc): return f"Product Description: {desc}"
def enrich_parent(parent): return f"Parent Category: {parent}"
def enrich_sales(sales): return f"Sales Category: {sales}"
