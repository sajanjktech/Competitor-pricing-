import os
import fitz  # PyMuPDF
import base64
import json
from dotenv import load_dotenv
from openai import AzureOpenAI

# ======================================================
# INITIAL SETUP
# ======================================================
load_dotenv()

INPUT_DIR = "input_files"
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version="2024-02-15-preview",
)

AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")


# ======================================================
# GLOBAL METADATA
# ======================================================
GLOBAL_DATA = {
    "competitor_name": "",
    "catalog_start": "",
    "catalog_end": ""
}


# ======================================================
# LOAD SYSTEM PROMPT
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
# VALUE CLEANER
# ======================================================
def clean_value(val):
    if val is None:
        return None
    if isinstance(val, str):
        return val.replace("’", "'").replace("‘", "'").strip()
    return val


# ======================================================
# UPDATE GLOBAL DATA
# ======================================================
def update_globals(parsed):
    if parsed.get("detected_competitor_name") and not GLOBAL_DATA["competitor_name"]:
        GLOBAL_DATA["competitor_name"] = parsed["detected_competitor_name"]

    if parsed.get("detected_catalog_effective_start") and not GLOBAL_DATA["catalog_start"]:
        GLOBAL_DATA["catalog_start"] = parsed["detected_catalog_effective_start"]

    if parsed.get("detected_catalog_effective_end") and not GLOBAL_DATA["catalog_end"]:
        GLOBAL_DATA["catalog_end"] = parsed["detected_catalog_effective_end"]


# ======================================================
# LLM CALL (PNG only – Azure Vision approved)
# ======================================================
def call_llm_png(png_bytes, prompt):
    b64 = base64.b64encode(png_bytes).decode("utf-8")
    data_url = f"data:image/png;base64,{b64}"

    response = client.chat.completions.create(
        model=AZURE_DEPLOYMENT,
        temperature=0,
        max_tokens=8000,
        messages=[
            {"role": "system", "content": prompt},
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": data_url}
                    }
                ]
            }
        ]
    )

    return response.choices[0].message.content


# ======================================================
# PARSE JSON
# ======================================================
def parse_json(output):
    if not output:
        return {"items": []}

    cleaned = output.strip().replace("```json", "").replace("```", "")
    try:
        return json.loads(cleaned)
    except Exception:
        return {"items": []}


# ======================================================
# PROCESS A SINGLE PDF PAGE
# ======================================================
def process_page(doc, idx):
    page = doc[idx]
    page_num = idx + 1

    # Extract raw text (can be empty for scanned pages)
    text = page.get_text("text").strip()
    prompt = build_prompt(text, page_num)

    # Convert every page to PNG (Azure Vision limitation)
    pix = page.get_pixmap(dpi=200)
    png_bytes = pix.tobytes("png")

    print(f"➡️ Page {page_num}: Sending as PNG → Azure Vision")

    try:
        llm_output = call_llm_png(png_bytes, prompt)
        parsed = parse_json(llm_output)
        update_globals(parsed)

        final_items = []
        for item in parsed.get("items", []):
            for k in item:
                item[k] = clean_value(item[k])
            final_items.append(item)

        return final_items

    except Exception as e:
        print(f"   ⚠ Error on page {page_num}: {e}")
        return []


# ======================================================
# PROCESS WHOLE PDF
# ======================================================
def process_pdf(pdf_path):
    global GLOBAL_DATA
    GLOBAL_DATA = {"competitor_name": "", "catalog_start": "", "catalog_end": ""}

    print(f"\n📄 Processing PDF → {pdf_path}")

    doc = fitz.open(pdf_path)
    all_items = []

    for i in range(len(doc)):
        items = process_page(doc, i)
        all_items.extend(items)

    output_path = os.path.join(
        OUTPUT_DIR,
        os.path.basename(pdf_path).replace(".pdf", "_items.json")
    )

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_items, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Extraction Complete → Saved: {output_path}")


# ======================================================
# PROCESS ALL PDFs
# ======================================================
def process_all_pdfs():
    for fname in os.listdir(INPUT_DIR):
        if fname.lower().endswith(".pdf"):
            process_pdf(os.path.join(INPUT_DIR, fname))


# ======================================================
# MAIN ENTRY POINT
# ======================================================
if __name__ == "__main__":
    process_all_pdfs()
