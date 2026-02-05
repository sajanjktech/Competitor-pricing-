# itemMappingAzure/freeMatcher.py

from itemMappingAzure.logger import logger
from itemMappingAzure.db_loader import load_gate_group_items, load_competitor_items
from itemMappingAzure.azureEmbedder import (
    get_embedding, enrich_name, enrich_desc, enrich_parent, enrich_sales
)

import math


# ---------------------------------------------------------
# COSINE SIMILARITY
# ---------------------------------------------------------
def cosine(v1, v2):
    return sum(a * b for a, b in zip(v1, v2))


# ---------------------------------------------------------
# EMBEDDING PIPELINE
# ---------------------------------------------------------
def embed_all_items():
    logger.info("🔄 Embedding GateGroup + Competitor items...")

    gg_rows = load_gate_group_items()
    comp_rows = load_competitor_items()

    gg_embeds = []
    comp_embeds = []

    # -----------------------------------------------
    # Gate Group Embeddings
    # -----------------------------------------------
    for g in gg_rows:
        name = g.item_onboard_name or ""
        desc = g.item_description or ""
        parent = g.item_parent_sales_category_name or ""
        sales = g.item_sales_category_name or ""

        gg_embeds.append({
            "id": g.item_row_id,
            "name": name,
            "desc": desc,
            "parent": parent,
            "sales": sales,

            "emb_name": get_embedding(enrich_name(name), f"gg_name_{g.item_row_id}", "gg"),
            "emb_desc": get_embedding(enrich_desc(desc), f"gg_desc_{g.item_row_id}", "gg"),
            "emb_parent": get_embedding(enrich_parent(parent), f"gg_parent_{g.item_row_id}", "gg"),
            "emb_sales": get_embedding(enrich_sales(sales), f"gg_sales_{g.item_row_id}", "gg")
        })

    # -----------------------------------------------
    # Competitor Item Embeddings
    # -----------------------------------------------
    for c in comp_rows:
        name = c.item_name or ""
        desc = c.Item_description or ""
        brand = c.brand or ""
        qty = c.quantity or ""
        parent = c.parent_category or ""
        sales = c.sales_category or ""

        full_name = f"{brand} {name} {qty}".strip()

        comp_embeds.append({
            "id": c.item_id,
            "name": name,
            "brand": brand,
            "quantity": qty,
            "desc": desc,
            "parent": parent,
            "sales": sales,
            "price": float(c.price) if c.price is not None else None,
            "currency": c.currency,

            # NEW → include competitor metadata directly from DB
            "competitor_name": c.competitor_name,
            "catalog_name": c.catalog_name,
            "catalog_start": str(c.catalog_start) if c.catalog_start else None,
            "catalog_end": str(c.catalog_end) if c.catalog_end else None,
            "page": c.page,

            # Embeddings
            "emb_name": get_embedding(enrich_name(full_name), f"comp_name_{c.item_id}", "comp"),
            "emb_desc": get_embedding(enrich_desc(desc), f"comp_desc_{c.item_id}", "comp"),
            "emb_parent": get_embedding(enrich_parent(parent), f"comp_parent_{c.item_id}", "comp"),
            "emb_sales": get_embedding(enrich_sales(sales), f"comp_sales_{c.item_id}", "comp")
        })

    return gg_embeds, comp_embeds


# ---------------------------------------------------------
# MATCHING PIPELINE
# ---------------------------------------------------------
def match_items_free():
    logger.info("🔍 Starting semantic matching...")

    gg_embeds, comp_embeds = embed_all_items()
    results = []
    NAME_WEIGHT = 0.70
    DESC_WEIGHT = 0.20
    PARENT_WEIGHT = 0.05
    SALES_WEIGHT = 0.05
    SIM_THRESHOLD = 0.75

    for gg in gg_embeds:
        matches = []

        for comp in comp_embeds:
            sim_name = cosine(gg["emb_name"], comp["emb_name"])
            sim_desc = cosine(gg["emb_desc"], comp["emb_desc"])
            sim_parent = cosine(gg["emb_parent"], comp["emb_parent"])
            sim_sales = cosine(gg["emb_sales"], comp["emb_sales"])

            final = (
                sim_name * NAME_WEIGHT +
                sim_desc * DESC_WEIGHT +
                sim_parent * PARENT_WEIGHT +
                sim_sales * SALES_WEIGHT
            )

            if final >= SIM_THRESHOLD:
                matches.append({
                    "competitor_item_id": comp["id"],
                    "competitor_item_name": comp["name"],
                    "brand": comp["brand"],
                    "quantity": comp["quantity"],
                    "competitor_description": comp["desc"],
                    "parent_category": comp["parent"],
                    "sales_category": comp["sales"],
                    "price": comp.get("price"),
                    "currency": comp.get("currency"),

                    "competitor_name": comp.get("competitor_name"),
                    "catalog_name": comp.get("catalog_name"),
                    "catalog_start": comp.get("catalog_start"),
                    "catalog_end": comp.get("catalog_end"),
                    "competitor_page": comp.get("page"),

                    "similarity_name": round(sim_name, 4),
                    "similarity_desc": round(sim_desc, 4),
                    "similarity_parent": round(sim_parent, 4),
                    "similarity_sales": round(sim_sales, 4),
                    "similarity_final": round(final, 4)
})
        matches.sort(key=lambda x: x["similarity_final"], reverse=True)

        if matches:
            results.append({
                "gate_item": {
                    "id": gg["id"],
                    "name": gg["name"],
                    "desc": gg["desc"],
                    "parent_category": gg["parent"],
                    "sales_category": gg["sales"]
                },
                "matches": matches
            })

    logger.info("✅ Matching complete.")
    return results
