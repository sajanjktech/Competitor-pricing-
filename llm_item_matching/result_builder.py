def build_final_results(llm_results, gg_items, comp_items):
    gg_lookup = {
        g.item_row_id: {
            "id": g.item_row_id,
            "name": g.item_name,
            "item_onboard_name": getattr(g, "item_onboard_name", None),
            "desc": g.item_description,
            "parent_category": getattr(g, "item_parent_sales_category_name", None),
            "sales_category": getattr(g, "item_sales_category_name", None)
        }
        for g in gg_items
    }

    comp_lookup = {
        c.item_id: {
            "id": c.item_id,
            "brand": c.brand,
            "name": c.item_name,
            "quantity": c.quantity,
            "desc": c.item_description,
            "parent_category": c.parent_category,
            "sales_category": c.sales_category,
            "price": float(c.price) if c.price is not None else None,
            "currency": c.currency,
            "competitor_name": c.competitor_name,
            "catalog_name": c.catalog_name,
            "catalog_start": str(c.catalog_start) if c.catalog_start else None,
            "catalog_end": str(c.catalog_end) if c.catalog_end else None,
            "competitor_page": c.page,
        }
        for c in comp_items
    }

    enriched = []

    for row in llm_results:

        gg_id = row["gate_item"]["id"]
        gg_full = gg_lookup.get(gg_id)

        enriched_matches = []

        for m in row["matches"]:
            comp_id = m["competitor_item_id"]
            comp_full = comp_lookup.get(comp_id)

            if not comp_full:
                continue

            enriched_matches.append({
                "competitor_item_id": comp_id,
                "competitor_item_name": comp_full["name"],
                "brand": comp_full["brand"],
                "quantity": comp_full["quantity"],
                "competitor_description": comp_full["desc"],
                "parent_category": comp_full["parent_category"],
                "sales_category": comp_full["sales_category"],
                "price": comp_full["price"],
                "currency": comp_full["currency"],
                "competitor_name": comp_full["competitor_name"],
                "catalog_name": comp_full["catalog_name"],
                "catalog_start": comp_full["catalog_start"],
                "catalog_end": comp_full["catalog_end"],
                "competitor_page": comp_full["competitor_page"],
                
                # NEW FIELDS from LLM
                "reasoning": m.get("reasoning", ""),
                "tags": m.get("tags", ""),

                # Similarity score
                "similarity_final": float(m["score"])
            })

        enriched.append({
            "gate_item": gg_full,
            "matches": enriched_matches
        })

    return enriched
