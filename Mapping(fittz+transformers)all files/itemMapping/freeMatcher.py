# itemMapping/freeMatcher.py

from itemMapping.logger import logger
from itemMapping.db_loader import load_gate_group_items, load_competitor_items
# from itemMapping.freeEmbedder import get_embedding
from itemMapping.azureEmbedder import get_embedding
import math


# -------------------------------------------------------------------
# COSINE SIMILARITY
# -------------------------------------------------------------------
def cosine(v1, v2):
    """Compute cosine similarity between two vectors."""
    dot = sum(a * b for a, b in zip(v1, v2))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))
    return dot / (norm1 * norm2) if norm1 > 0 and norm2 > 0 else 0


# -------------------------------------------------------------------
# PRICE HELPERS
# -------------------------------------------------------------------
def parse_gate_price(price_string):
    if price_string is None:
        return (0.0, 0.0)
    try:
        parts = str(price_string).replace("£", "").split("-")
        if len(parts) == 2:
            return (float(parts[0]), float(parts[1]))
        else:
            return (float(parts[0]), float(parts[0]))
    except:
        return (0.0, 0.0)


def safe_float(value):
    if value is None:
        return 0.0
    try:
        return float(str(value).replace("£", "").strip())
    except:
        return 0.0


# -------------------------------------------------------------------
# EMBEDDING PIPELINE
# -------------------------------------------------------------------
def embed_all_items():
    """Embed all Gategroup + competitor items using local model."""

    logger.info("Embedding items with weighted model...")

    gg_items = load_gate_group_items()
    comp_items = load_competitor_items()

    gg_embeds = []
    comp_embeds = []

    # ---- GateGroup items ----
    for idx, g in enumerate(gg_items, start=1):
        name = g.item_onboard_name or ""
        desc = g.item_description or ""

        logger.info(f"[GG] Embedding {idx}/{len(gg_items)}: {name}")

        gg_embeds.append({
            "id": g.item_row_id,
            "name": name,
            "desc": desc,
            "price": parse_gate_price(g.item_price),
            "currency": g.item_currency_code,

            # Separate embeddings for name + description
            "embed_name": get_embedding(name),
            "embed_desc": get_embedding(desc),
        })

    # ---- Competitor items ----
    for idx, c in enumerate(comp_items, start=1):
        brand = getattr(c, "Item_brand", "") or ""
        qty = getattr(c, "Item_Quantity", "") or ""
        name = c.Item_name or ""
        desc = c.Item_description or ""

        logger.info(f"[COMP] Embedding {idx}/{len(comp_items)}: {name}")

        comp_embeds.append({
            "id": c.id,
            "brand": brand,
            "quantity": qty,
            "name": name,
            "desc": desc,
            "price": safe_float(c.Item_price),
            "currency": c.Item_currency,

            # Competitor name field = brand + name + qty
            "embed_name": get_embedding(f"{brand} {name} {qty}".strip()),
            "embed_desc": get_embedding(desc),
        })

    return gg_embeds, comp_embeds


# -------------------------------------------------------------------
# MATCHING PIPELINE
# -------------------------------------------------------------------
def match_items_free():
    logger.info("Starting weighted cosine similarity matching...")

    gg_embeds, comp_embeds = embed_all_items()
    results = []

    NAME_WEIGHT = 0.90
    DESC_WEIGHT = 0.10
    SIM_THRESHOLD = 0.70

    for idx, gg in enumerate(gg_embeds, start=1):
        logger.info(f"[MATCH] {idx}/{len(gg_embeds)} → {gg['name']}")

        matches = []

        for comp in comp_embeds:

            # Compute weighted similarity
            sim_name = cosine(gg["embed_name"], comp["embed_name"])
            sim_desc = cosine(gg["embed_desc"], comp["embed_desc"])

            final_similarity = (
                (NAME_WEIGHT * sim_name) +
                (DESC_WEIGHT * sim_desc)
            )

            if final_similarity >= SIM_THRESHOLD:
                matches.append({
                    "competitor_item_id": comp["id"],
                    "competitor_item_name": comp["name"],
                    "brand": comp["brand"],
                    "quantity": comp["quantity"],
                    "competitor_description": comp["desc"],
                    "competitor_price": comp["price"],
                    "competitor_currency": comp["currency"],
                    "similarity_name": round(sim_name, 4),
                    "similarity_desc": round(sim_desc, 4),
                    "similarity_final": round(final_similarity, 4),
                })

        matches = sorted(matches, key=lambda x: x["similarity_final"], reverse=True)

        if matches:
            results.append({
                "gate_item": {
                    "id": gg["id"],
                    "name": gg["name"],
                    "desc": gg["desc"],
                    "price_range_gbp": gg["price"],
                    "currency": gg["currency"],
                },
                "matches": matches
            })

    logger.info("Matching completed with weighted similarity.")
    return results
