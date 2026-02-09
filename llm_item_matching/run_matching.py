# run_matching.py

import json
from db_loader import load_gate_group_items, load_competitor_items
from llm_matcher import match_all_items_llm
from result_builder import build_final_results


def run():
    print("Loading GG and competitor items...")
    gg_items = load_gate_group_items()
    comp_items = load_competitor_items()

    print(f"GG Items Loaded: {len(gg_items)}")
    print(f"Competitor Items Loaded: {len(comp_items)}")

    print("Running LLM matching...")
    llm_results = match_all_items_llm(gg_items, comp_items)

    print("Building FINAL enriched result...")
    final_results = build_final_results(llm_results, gg_items, comp_items)

    print("Saving results to llm_results.json...")
    with open("llm_results.json", "w", encoding="utf-8") as f:
        json.dump(final_results, f, indent=2, ensure_ascii=False)

    print("Done! FINAL output saved to llm_results.json.")


if __name__ == "__main__":
    run()
