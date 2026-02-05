# matcher.py
from itemMapping.logger import logger
from itemMapping.embedder import embed_text, cosine
from itemMapping.db_loader import load_gate_group_items, load_competitor_items

def embed_all_items():
    logger.info("Embedding GateGroup + Competitor items...")

    gg_items = load_gate_group_items()
    comp_items = load_competitor_items()

    gg_embeds = []
    comp_embeds = []

    for idx, g in enumerate(gg_items, start=1):
        logger.info(f"[GG] Embedding {idx}/{len(gg_items)}: {g.item_name}")
        vec = embed_text(f"{g.item_name} - {g.item_description}")
        if vec:
            gg_embeds.append({
                "id": g.item_row_id,
                "name": g.item_name,
                "desc": g.item_description,
                "price": g.item_price,
                "currency": g.item_currency_code,
                "embed": vec
            })

    for idx, c in enumerate(comp_items, start=1):
        logger.info(f"[COMP] Embedding {idx}/{len(comp_items)}: {c.Item_name}")
        vec = embed_text(f"{c.Item_name} - {c.Item_description}")
        if vec:
            comp_embeds.append({
                "id": c.id,
                "name": c.Item_name,
                "desc": c.Item_description,
                "price": c.Item_price,
                "currency": c.Item_currency,
                "embed": vec
            })

    return gg_embeds, comp_embeds


def match_items():
    logger.info("Starting item matching...")

    gg_embeds, comp_embeds = embed_all_items()
    results = []

    for idx, comp in enumerate(comp_embeds, start=1):
        logger.info(f"[MATCH] {idx}/{len(comp_embeds)} → {comp['name']}")
        sims = []

        for gg in gg_embeds:
            score = cosine(comp["embed"], gg["embed"])
            sims.append({
                "gate_item_id": gg["id"],
                "gate_item_name": gg["name"],
                "gate_item_description": gg["desc"],
                "gate_item_price": gg["price"],
                "gate_item_currency": gg["currency"],
                "similarity": score
            })

        top5 = sorted(sims, key=lambda x: x["similarity"], reverse=True)[:5]
        results.append({
            "competitor_item": comp,
            "matches": top5
        })

    logger.info("Matching completed.")
    return results