# itemMapping/freeMatcher.py

from itemMapping.logger import logger
from itemMapping.db_loader import load_gate_group_items, load_competitor_items
from itemMapping.freeEmbedder import get_embedding
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
    """
    KEEPS minimum and maximum price from GateGroup as a tuple.
    Example: "5.5 - 6.5" → (5.5, 6.5)
    """
    if price_string is None:
        return (0.0, 0.0)
    try:
        parts = str(price_string).replace("£", "").split("-")
        if len(parts) == 2:
            return (float(parts[0]), float(parts[1]))
        else:
            # single value
            return (float(parts[0]), float(parts[0]))
    except:
        return (0.0, 0.0)


def safe_float(value):
    """
    Convert competitor prices to safe float.
    - Handles NULL
    - Handles "FREE"
    - Handles text
    - Handles £ symbols
    """
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

    logger.info("Embedding items (FREE mode)...")

    gg_items = load_gate_group_items()
    comp_items = load_competitor_items()

    gg_embeds = []
    comp_embeds = []

    # ---- GateGroup items ----
    for idx, g in enumerate(gg_items, start=1):
        logger.info(f"[GG] Embedding {idx}/{len(gg_items)}: {g.item_onboard_name}")

        # full_text = f"{g.item_onboard_name} - {g.item_description}"
        full_text = f"{g.item_onboard_name}"
        vec = get_embedding(full_text)

        gg_embeds.append({
            "id": g.item_row_id,
            "name": g.item_onboard_name,
            "desc": g.item_description,
            "price": parse_gate_price(g.item_price),  # (min,max)
            "currency": g.item_currency_code,
            "embed": vec
        })

    # ---- Competitor items ----
    for idx, c in enumerate(comp_items, start=1):
        logger.info(f"[COMP] Embedding {idx}/{len(comp_items)}: {c.Item_name}")

        # full_text = f"{c.Item_name} - {c.Item_description}"
        full_text = f"{c.Item_name}"
        vec = get_embedding(full_text)

        comp_embeds.append({
            "id": c.id,
            "name": c.Item_name,
            "desc": c.Item_description,
            "price": safe_float(c.Item_price),
            "currency": c.Item_currency,
            "embed": vec
        })

    return gg_embeds, comp_embeds


# -------------------------------------------------------------------
# MATCHING PIPELINE
# -------------------------------------------------------------------
def match_items_free():
    """Match Gategroup items → competitor items using cosine similarity."""
    logger.info("Starting FREE item matching...")

    gg_embeds, comp_embeds = embed_all_items()
    results = []

    SIM_THRESHOLD = 0.70  # Only return matches ≥ 50%

    # Loop GateGroup FIRST
    for idx, gg in enumerate(gg_embeds, start=1):
        logger.info(f"[MATCH] {idx}/{len(gg_embeds)} → {gg['name']}")

        matches = []

        for comp in comp_embeds:
            score = cosine(gg["embed"], comp["embed"])

            if score >= SIM_THRESHOLD:
                matches.append({
                    "competitor_item_id": comp["id"],
                    "competitor_item_name": comp["name"],
                    "competitor_description": comp["desc"],
                    "competitor_price": comp["price"],
                    "competitor_currency": comp["currency"],
                    "similarity": round(score, 4)
                })

        # Sort by similarity DESC
        matches = sorted(matches, key=lambda x: x["similarity"], reverse=True)

        # Only include items that HAVE matches
        if matches:
            results.append({
                "gate_item": {
                    "id": gg["id"],
                    "name": gg["name"],
                    "desc": gg["desc"],
                    "price_range_gbp": gg["price"],
                    "currency": gg["currency"]
                },
                "matches": matches
            })

    logger.info("FREE matching completed.")
    return results