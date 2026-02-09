# category_runner.py

import json
from db_loader import load_gate_group_items, load_competitor_items
from llm_matcher import match_all_items_llm
from result_builder import build_final_results

# ---------------------------------------------------------
# DEFINE CATEGORY GROUPS
# Each entry maps: visible label → list of categories in DB
# ---------------------------------------------------------
CATEGORY_GROUPS = {
    # "Alcohol_DFSpirits": ["Alcohol", "DF Spirits"],

    # "Fragrance": ["Ladies Fragrance", "Gents Fragrance"],

    "Snacks": ["Sweet Snacks", "Savoury Snacks"]

    # "Drinks": ["Cold Drink", "Hot Drink"],

    # "Accessories": ["Accessories"],

    # "Skincare_Makeup": ["Skincare & Make-up"],

    # "FreshFood_Combos": ["Fresh Food", "Combos"],

    # "Gadgets": ["Gadgets"],

    # "Tobacco": ["Tobacco"],

    # "Tickets_Logo": ["Tickets", "Logo"]
}

# ---------------------------------------------------------
# CATEGORY FILTER HELPERS
# ---------------------------------------------------------
def filter_gg_items(gg_items, categories):
    return [
        g for g in gg_items
        if getattr(g, "item_sales_category_name", None) in categories
    ]

def filter_comp_items(comp_items, categories):
    return [
        c for c in comp_items
        if getattr(c, "sales_category", None) in categories
    ]

# ---------------------------------------------------------
# MAIN CATEGORY RUNNER
# ---------------------------------------------------------
def run_category_matching():
    print("Loading FULL GG list...")
    gg_all = load_gate_group_items()

    print("Loading FULL competitor list...")
    comp_all = load_competitor_items()

    print(f"Total GG items loaded   : {len(gg_all)}")
    print(f"Total COMP items loaded : {len(comp_all)}\n")

    for group_name, categories in CATEGORY_GROUPS.items():
        print("\n====================================================")
        print(f"🔍 CATEGORY GROUP → {group_name} ({categories})")
        print("====================================================")

        gg = filter_gg_items(gg_all, categories)
        comp = filter_comp_items(comp_all, categories)

        print(f"✔ Filtered GG items: {len(gg)}")
        print(f"✔ Filtered COMP items: {len(comp)}")

        if len(gg) == 0 or len(comp) == 0:
            print("⚠ Skipping — empty category data.")
            continue

        print("➡ Running LLM matching...")
        llm_raw = match_all_items_llm(gg, comp)

        print("➡ Building final enriched results...")
        final_result = build_final_results(llm_raw, gg, comp)

        output_path = f"llm_results_{group_name}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(final_result, f, indent=2, ensure_ascii=False)

        print(f"✅ Saved: {output_path}")


if __name__ == "__main__":
    run_category_matching()
