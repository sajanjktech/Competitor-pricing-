# main.py
import json
import time
import decimal
from itemMapping.logger import logger
from itemMapping.freeMatcher import match_items_free as match_items
# from itemMapping.matcher import match_items(for PAID mode)azure


def convert_decimal(obj):
    """Convert Decimal → float for JSON serialization"""
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return obj


if __name__ == "__main__":
    start = time.time()
    logger.info("🚀 Starting mapping pipeline...")

    try:
        # Run the full matching process
        result = match_items()

        # Save JSON output
        with open("price_comparison_matches.json", "w", encoding="utf-8") as f:
            json.dump(
                result, f, indent=2, ensure_ascii=False, default=convert_decimal
            )

        logger.info("💾 Output saved → price_comparison_matches.json")

    except Exception as e:
        logger.error("❌ Pipeline FAILED")
        logger.error(e, exc_info=True)

    logger.info(f"⏳ Runtime: {round(time.time() - start, 2)} sec")
    logger.info("🎉 Completed.")