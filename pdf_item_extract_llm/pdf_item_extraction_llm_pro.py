import os
import fitz
import base64
import json
import time
import logging
import concurrent.futures
from dotenv import load_dotenv
from openai import AzureOpenAI

# ======================================================
# INITIAL SETUP
# ======================================================
load_dotenv()

INPUT_DIR = os.path.join(os.path.dirname(__file__), "input_files")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "llm_output")
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")

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
THREAD_COUNT = 5

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
        page_num=page_num
    )

# ======================================================
# DATE NORMALIZATION
# ======================================================
SEASON_MAP = {
    "WINTER": ("12-01", "02-28"),
    "SPRING": ("03-01", "05-31"),
    "SUMMER": ("06-01", "08-31"),
    "AUTUMN": ("09-01", "11-30"),
    "FALL": ("09-01", "11-30"),
}

def convert_seasonal(date_str):
    if not date_str or str(date_str).strip() == "":
        return None, None

    original = date_str.strip()
    upper = original.upper().replace("’", "'")

    for season, (start_suffix, end_suffix) in SEASON_MAP.items():
        if season in upper:
            year = "".join(filter(str.isdigit, upper))
            if len(year) == 2:
                year = "20" + year
            if len(year) == 0:
                return None, None
            return f"{year}-{start_suffix}", f"{year}-{end_suffix}"

    return original, original

# ======================================================
# JSON PARSER
# ======================================================
def safe_parse(raw):
    if not raw:
        return {}

    try:
        start = raw.find("{")
        end = raw.rfind("}") + 1
        return json.loads(raw[start:end])
    except:
        return {}

# ======================================================
# LLM CALL
# ======================================================
def call_azure_with_retry(data_url, prompt, retries=5):
    for attempt in range(1, retries + 1):
        try:
            r = client.chat.completions.create(
                model=AZURE_DEPLOYMENT,
                temperature=0,
                max_tokens=6000,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": [
                        {"type": "image_url", "image_url": {"url": data_url}}
                    ]}
                ]
            )
            return r.choices[0].message.content

        except Exception as e:
            logging.warning(f"[Retry {attempt}] {e}")
            time.sleep(attempt * 1.5)

    return ""

# ======================================================
# PROCESS ONE PAGE
# ======================================================
def process_single_page(pdf_path, idx, catalog_name):
    doc = fitz.open(pdf_path)
    page = doc[idx]
    page_num = idx + 1

    text = page.get_text("text").strip()
    prompt = build_prompt(text, page_num)

    pix = page.get_pixmap(dpi=200)
    png_bytes = pix.tobytes("png")
    data_url = "data:image/png;base64," + base64.b64encode(png_bytes).decode()

    total = fitz.open(pdf_path).page_count if hasattr(fitz.open(pdf_path), "page_count") else "?"
    print(f"➡️ Page {page_num}: Sending to LLM…")

    raw = call_azure_with_retry(data_url, prompt)

    with open(os.path.join(LOG_DIR, f"page_{page_num}_raw.txt"), "w", encoding="utf-8") as f:
        f.write(raw or "")

    parsed = safe_parse(raw)

    # Extract metadata from this page
    comp = parsed.get("detected_competitor_name")
    start = parsed.get("detected_catalog_effective_start")
    end = parsed.get("detected_catalog_effective_end")

    # seasonal conversion
    start, _ = convert_seasonal(start)
    _, end = convert_seasonal(end)

    items_out = []

    for item in parsed.get("items", []):
        out = {
            "catalog_name": catalog_name,
            "competitor_name": comp,
            "catalog_start": start,
            "catalog_end": end,

            "item_name": item.get("Item_name"),
            "description": item.get("Item_description"),
            "brand": item.get("Item_brand"),
            "quantity": item.get("Item_Quantity"),

            "parent_category": item.get("Item_parent_category"),
            "sales_category": item.get("Item_sales_category"),

            "price": item.get("Item_price"),
            "currency": item.get("Item_currency"),
            "page": page_num
        }

        # normalize nulls
        for k, v in out.items():
            if v in ["", "none", "null", None]:
                out[k] = None

        # numeric price
        if out["price"] is not None:
            try:
                out["price"] = float(out["price"])
            except:
                out["price"] = None

        items_out.append(out)

    print(f"✔ Page {page_num} done ({len(items_out)} items)")
    return {
        "page_num": page_num,
        "competitor": comp,
        "start": start,
        "end": end,
        "items": items_out
    }

# ======================================================
# FINAL PDF PROCESSOR
# ======================================================
def process_pdf(pdf_path):
    print(f"\n📄 Processing PDF: {pdf_path}")
    filename = os.path.basename(pdf_path)
    catalog_name = filename.replace(".pdf", "")

    doc = fitz.open(pdf_path)
    pages = len(doc)

    page_results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_COUNT) as exe:
        futures = [
            exe.submit(process_single_page, pdf_path, i, catalog_name)
            for i in range(pages)
        ]
        for f in concurrent.futures.as_completed(futures):
            page_results.append(f.result())

    # consolidate metadata
    competitor = next((r["competitor"] for r in page_results if r["competitor"]), None)
    catalog_start = next((r["start"] for r in page_results if r["start"]), None)
    catalog_end = next((r["end"] for r in page_results if r["end"]), None)

    print("\n🔍 Consolidating metadata across pages...")
    print(f"✔ Competitor found: {competitor}")
    print(f"✔ Catalog Start: {catalog_start}")
    print(f"✔ Catalog End: {catalog_end}")


    # inject final metadata into every item
    final_items = []
    for r in page_results:
        for item in r["items"]:
            item["competitor_name"] = competitor
            item["catalog_start"] = catalog_start
            item["catalog_end"] = catalog_end
            final_items.append(item)

    # sort correctly
    final_items = sorted(final_items, key=lambda x: x["page"])

    out_path = os.path.join(OUTPUT_DIR, f"{catalog_name}_items.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(final_items, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved → {out_path}")

# ======================================================
# MAIN
# ======================================================
def process_all_pdfs():
    for f in os.listdir(INPUT_DIR):
        if f.lower().endswith(".pdf"):
            process_pdf(os.path.join(INPUT_DIR, f))

if __name__ == "__main__":
    process_all_pdfs()
