# main.py

import json
import time
import decimal
from itemMappingAzure.logger import logger
from itemMappingAzure.freeMatcher import match_items_free

def convert_decimal(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return obj

if __name__ == "__main__":
    start = time.time()
    logger.info("🚀 Starting Azure semantic matching pipeline...")

    result = match_items_free()

    with open("price_comparison_matches.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False, default=convert_decimal)

    logger.info(f"⏳ Runtime: {round(time.time() - start, 2)} sec")
    logger.info("🎉 Completed.")
