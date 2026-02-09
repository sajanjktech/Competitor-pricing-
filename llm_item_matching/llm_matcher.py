# llm_matcher.py

import json
from system_prompt import SYSTEM_PROMPT
from llm_client import call_llm

GG_BATCH_SIZE = 5
COMP_BATCH_SIZE = 20


def round_scores(matches):
    """
    Convert LLM score → 2-decimal string.
    Ensure new fields exist: reasoning, tags.
    """
    for m in matches:
        try:
            score_val = float(m.get("score", 0))
        except:
            score_val = 0.0
        m["score"] = format(score_val, ".2f")

        m.setdefault("reasoning", "")
        m.setdefault("tags", "")

    return matches


def batch_list(data, size):
    for i in range(0, len(data), size):
        yield data[i:i + size]


def match_batch(gate_items_batch, competitor_items_batch):
    payload = {
        "gate_items": gate_items_batch,
        "competitor_items": competitor_items_batch
    }

    response = call_llm(SYSTEM_PROMPT, payload)

    print("\n================ RAW LLM OUTPUT ================")
    print(json.dumps(response, indent=2, ensure_ascii=False))
    print("================================================\n")

    if not isinstance(response, dict) or "items" not in response:
        raise ValueError(
            f"Invalid LLM output: Expected {'items': [...]} but got: {response}"
        )

    return response["items"]


def match_all_items_llm(gg_items, comp_items):
    final_results = []

    gg_minimal = [
        {"id": g.item_row_id, "name": g.item_name or "", "desc": g.item_description or ""}
        for g in gg_items
    ]

    comp_minimal = [
        {"id": c.item_id, "brand": c.brand or "", "name": c.item_name or "",
         "qty": c.quantity or "", "desc": c.item_description or ""}
        for c in comp_items
    ]

    gg_batches = list(batch_list(gg_minimal, GG_BATCH_SIZE))
    comp_batches = list(batch_list(comp_minimal, COMP_BATCH_SIZE))

    for gg_batch in gg_batches:

        merged = {g["id"]: [] for g in gg_batch}

        for comp_batch in comp_batches:
            batch_output = match_batch(gg_batch, comp_batch)

            for item_result in batch_output:
                gg_id = item_result["gate_item_id"]
                matches = round_scores(item_result.get("matches", []))
                merged[gg_id].extend(matches)

        # ----------- FINAL MERGE -----------
        for g in gg_batch:
            gg_id = g["id"]

            # NEW RULE: Skip empty match list
            if not merged[gg_id]:
                continue

            final_results.append({
                "gate_item": {
                    "id": gg_id,
                    "name": g["name"]
                },
                "matches": sorted(
                    merged[gg_id],
                    key=lambda x: float(x["score"]),
                    reverse=True
                )
            })

    return final_results
