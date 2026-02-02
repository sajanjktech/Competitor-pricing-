import os
import fitz  # PyMuPDF
import base64
import json
import time
import logging
import threading
import concurrent.futures
from dotenv import load_dotenv
from openai import AzureOpenAI

# ======================================================
# INITIAL SETUP
# ======================================================
load_dotenv()

INPUT_DIR = "input_files"
OUTPUT_DIR = "output"
LOG_DIR = "logs"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "run.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version="2024-02-15-preview",
)

AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
THREAD_COUNT = 6


# ======================================================
# GLOBAL METADATA (will autofill missing items)
# ======================================================
GLOBAL_DATA = {
    "competitor_name": "",
    "catalog_start": "",
    "catalog_end": ""
}

# ======================================================
# SYSTEM PROMPT
# ======================================================
def load_system_prompt():
    with open("system_prompt.txt", "r", encoding="utf-8") as f:
        return f.read()

SYSTEM_PROMPT_TEMPLATE = load_system_prompt()

def build_prompt(page_text, page_num):
    return SYSTEM_PROMPT_TEMPLATE.format(
        page_text=page_text or "",
        page_num=page_num,
        competitor_name=GLOBAL_DATA["competitor_name"],
        catalog_effective_start=GLOBAL_DATA["catalog_start"],
        catalog_effective_end=GLOBAL_DATA["catalog_end"],
    )

# ======================================================
# FIELD NORMALIZATION + METADATA AUTO-INJECTION
# ======================================================
FIELD_MAP = {
    "Competitor_Name": "competitor_name",
    "Catalog_effective_start_date": "catalog_start",
    "Catalog_effective_end_date": "catalog_end",

    "Item_name": "item_name",
    "Item_description": "description",
    "Item_brand": "brand",
    "Item_Quantity": "quantity",
    "Item_parent_category": "parent_category",
    "Item_sales_category": "sales_category",
    "Item_price": "price",
    "Item_currency": "currency",
    "menu_page": "page",
}

def normalize_item(raw_item):
    normalized = {}

    for key, val in raw_item.items():
        final_key = FIELD_MAP.get(key, key.lower().strip())
        normalized[final_key] = clean_value(val)

    # ⭐ FIX 1 — Auto-fill missing metadata from GLOBAL_DATA
    if not normalized.get("competitor_name"):
        normalized["competitor_name"] = GLOBAL_DATA["competitor_name"]

    if not normalized.get("catalog_start"):
        normalized["catalog_start"] = GLOBAL_DATA["catalog_start"]

    if not normalized.get("catalog_end"):
        normalized["catalog_end"] = GLOBAL_DATA["catalog_end"]

    return normalized

def clean_value(val):
    if val is None:
        return None
    if isinstance(val, str):
        return val.replace("’", "'").replace("‘", "'").strip()
    return val

def normalize_price(item):
    if not item.get("price"):
        return item

    price_str = str(item["price"]).replace(",", "").replace("£", "").strip()

    try:
        item["price"] = float(price_str)
        item["currency"] = item.get("currency", "GBP")
    except:
        pass

    return item


# ======================================================
# JSON HANDLING
# ======================================================
def safe_json_parse(raw):
    if not raw:
        return {"items": []}

    cleaned = raw.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(cleaned)
    except:
        return {"items": []}

def update_globals(parsed):
    if parsed.get("detected_competitor_name") and not GLOBAL_DATA["competitor_name"]:
        GLOBAL_DATA["competitor_name"] = parsed["detected_competitor_name"]

    if parsed.get("detected_catalog_effective_start") and not GLOBAL_DATA["catalog_start"]:
        GLOBAL_DATA["catalog_start"] = parsed["detected_catalog_effective_start"]

    if parsed.get("detected_catalog_effective_end") and not GLOBAL_DATA["catalog_end"]:
        GLOBAL_DATA["catalog_end"] = parsed["detected_catalog_effective_end"]

# ======================================================
# RETRY LLM CALL
# ======================================================
def call_azure_with_retry(data_url, prompt, retries=4, delay=1):
    for attempt in range(1, retries + 1):
        try:
            response = client.chat.completions.create(
                model=AZURE_DEPLOYMENT,
                temperature=0,
                max_tokens=7000,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": [
                        {"type": "image_url", "image_url": {"url": data_url}}
                    ]}
                ]
            )
            return response.choices[0].message.content

        except Exception as e:
            logging.warning(f"Retry {attempt}: {e}")
            time.sleep(delay * attempt)

    return ""

# ======================================================
# PROCESS ONE PAGE
# ======================================================
def process_single_page(doc_path, idx):
    doc = fitz.open(doc_path)
    page = doc[idx]
    page_num = idx + 1

    text = page.get_text("text").strip()
    prompt = build_prompt(text, page_num)

    pix = page.get_pixmap(dpi=200)
    png_bytes = pix.tobytes("png")
    b64 = base64.b64encode(png_bytes).decode()
    data_url = f"data:image/png;base64,{b64}"

    print(f"➡️ Page {page_num}: Extracting…")

    raw = call_azure_with_retry(data_url, prompt)

    # Log raw LLM output
    with open(os.path.join(LOG_DIR, f"page_{page_num}_raw.txt"), "w", encoding="utf-8") as f:
        f.write(raw or "")

    parsed = safe_json_parse(raw)
    update_globals(parsed)

    final_items = []

    # Normalize all items
    for item in parsed.get("items", []):
        norm = normalize_item(item)
        norm = normalize_price(norm)
        norm["page"] = page_num
        final_items.append(norm)

    return final_items


# ======================================================
# PROCESS FULL PDF
# ======================================================
def process_pdf(pdf_path):
    global GLOBAL_DATA
    GLOBAL_DATA = {"competitor_name": "", "catalog_start": "", "catalog_end": ""}

    print(f"\n📄 Processing {pdf_path}")
    logging.info(f"Processing {pdf_path}")

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    all_items = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = {
            executor.submit(process_single_page, pdf_path, i): i
            for i in range(total_pages)
        }

        for future in concurrent.futures.as_completed(futures):
            items = future.result()
            all_items.extend(items)

    # ⭐ FIX 3 — Sort items page-wise
    all_items = sorted(all_items, key=lambda x: x.get("page", 0))

    out_path = os.path.join(
        OUTPUT_DIR,
        os.path.basename(pdf_path).replace(".pdf", "_items.json")
    )

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_items, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved → {out_path}")
    logging.info(f"Saved → {out_path}")


# ======================================================
# MAIN
# ======================================================
def process_all_pdfs():
    for file in os.listdir(INPUT_DIR):
        if file.lower().endswith(".pdf"):
            process_pdf(os.path.join(INPUT_DIR, file))

if __name__ == "__main__":
    process_all_pdfs()
