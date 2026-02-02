import os
import json
from dotenv import load_dotenv
from openai import AzureOpenAI

load_dotenv()

# ======================================================
# Azure Client
# ======================================================
client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version="2024-02-15-preview"
)

AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")

EXTRACTED_DIR = "extracted"
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# GLOBAL state shared across all pages
GLOBAL_COMPETITOR_NAME = ""
GLOBAL_CATALOG_EFFECTIVE_START = ""
GLOBAL_CATALOG_EFFECTIVE_END = ""

# ======================================================
# Load system prompt template
# ======================================================
def load_system_prompt():
    with open("system_prompt.txt", "r", encoding="utf-8") as f:
        return f.read()

SYSTEM_PROMPT_TEMPLATE = load_system_prompt()


# ======================================================
# Build prompt with updated fields
# ======================================================
def build_prompt(page_text, page_num):
    return SYSTEM_PROMPT_TEMPLATE.format(
        page_text=page_text,
        page_num=page_num,
        competitor_name=GLOBAL_COMPETITOR_NAME or "",
        catalog_effective_start=GLOBAL_CATALOG_EFFECTIVE_START or "",
        catalog_effective_end=GLOBAL_CATALOG_EFFECTIVE_END or ""
    )


# ======================================================
# Normalize text values
# ======================================================
def clean_value(val):
    if val is None:
        return None
    if not isinstance(val, str):
        return val
    return (
        val.replace("’", "'")
           .replace("‘", "'")
           .strip()
    )


# ======================================================
# Update global competitor name & catalog dates
# ======================================================
def update_global_details(detected_name, detected_start, detected_end):
    global GLOBAL_COMPETITOR_NAME
    global GLOBAL_CATALOG_EFFECTIVE_START
    global GLOBAL_CATALOG_EFFECTIVE_END

    if detected_name and not GLOBAL_COMPETITOR_NAME:
        GLOBAL_COMPETITOR_NAME = detected_name

    if detected_start and not GLOBAL_CATALOG_EFFECTIVE_START:
        GLOBAL_CATALOG_EFFECTIVE_START = detected_start

    if detected_end and not GLOBAL_CATALOG_EFFECTIVE_END:
        GLOBAL_CATALOG_EFFECTIVE_END = detected_end


# ======================================================
# LLM CALL
# ======================================================
def call_llm(page_text, page_number):
    response = client.chat.completions.create(
        model=AZURE_DEPLOYMENT,
        messages=[{"role": "user", "content": build_prompt(page_text, page_number)}],
        temperature=0,
        max_tokens=8000
    )
    return response.choices[0].message.content


# ======================================================
# Extract JSON structure from LLM response
# ======================================================
def extract_json(output):
    cleaned = output.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(cleaned)
    except Exception:
        return {
            "detected_competitor_name": "",
            "detected_catalog_effective_start": "",
            "detected_catalog_effective_end": "",
            "items": []
        }


# ======================================================
# Page selection logic
# ======================================================
def should_process_page(page_num, pages_to_process):
    if pages_to_process == "all":
        return True
    if isinstance(pages_to_process, list):
        return page_num in pages_to_process
    return True


# ======================================================
# Process one file
# ======================================================
def process_jsonl_file(filename, pages_to_process="all"):

    global GLOBAL_COMPETITOR_NAME
    global GLOBAL_CATALOG_EFFECTIVE_START
    global GLOBAL_CATALOG_EFFECTIVE_END

    # Reset global state
    GLOBAL_COMPETITOR_NAME = ""
    GLOBAL_CATALOG_EFFECTIVE_START = ""
    GLOBAL_CATALOG_EFFECTIVE_END = ""

    input_path = os.path.join(EXTRACTED_DIR, filename)
    output_path = os.path.join(OUTPUT_DIR, filename.replace(".jsonl", "_combined.json"))

    print(f"\n📄 Processing: {filename}")

    final_items = []

    # Load all lines (pages)
    with open(input_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for line in lines:
        entry = json.loads(line)
        page_num = entry["page"]
        page_text = entry["text"].strip()

        if not should_process_page(page_num, pages_to_process):
            print(f"⏭️ Skipping page {page_num}")
            continue

        if not page_text:
            print(f"➡️ Page {page_num}: EMPTY → []")
            continue

        print(f"➡️ Processing Page {page_num}...")

        llm_output = call_llm(page_text, page_num)
        parsed = extract_json(llm_output)

        # Update global extracted metadata
        update_global_details(
            parsed.get("detected_competitor_name"),
            parsed.get("detected_catalog_effective_start"),
            parsed.get("detected_catalog_effective_end")
        )

        # Extract items
        items = parsed.get("items", [])

        # Clean item values
        for item in items:
            for key in item:
                item[key] = clean_value(item[key])

        final_items.extend(items)

    # Save JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_items, f, indent=2, ensure_ascii=False)

    print("\n✅ Extraction complete!")
    print(f"📦 Saved to: {output_path}")


# ======================================================
# Main
# ======================================================
if __name__ == "__main__":
    process_jsonl_file("Jet2.pdf.jsonl", pages_to_process="all")