import os
import json
import re
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


# ======================================================
# Load system prompt from external file
# ======================================================
def load_system_prompt():
    with open("system_prompt.txt", "r", encoding="utf-8") as f:
        return f.read()

SYSTEM_PROMPT_TEMPLATE = load_system_prompt()


# ======================================================
# Build full prompt with injected values
# ======================================================
def build_prompt(page_text, page_num):
    return SYSTEM_PROMPT_TEMPLATE.format(
        page_text=page_text,
        page_num=page_num
    )


# ======================================================
# Normalize values (apostrophes, spacing)
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
# LLM CALL
# ======================================================
def call_llm(text, page_number):
    response = client.chat.completions.create(
        model=AZURE_DEPLOYMENT,
        messages=[{"role": "user", "content": build_prompt(text, page_number)}],
        temperature=0,
        max_tokens=8000
    )
    return response.choices[0].message.content


# ======================================================
# JSON Extractor
# ======================================================
def extract_json(output):
    cleaned = output.replace("```json", "").replace("```", "").strip()
    start = cleaned.find("[")
    end = cleaned.rfind("]")

    if start == -1 or end == -1:
        return []

    try:
        return json.loads(cleaned[start:end+1])
    except:
        return []


# ======================================================
# Pipeline: Process .jsonl Page-By-Page
# ======================================================
def process_jsonl_file(filename):

    input_path = os.path.join(EXTRACTED_DIR, filename)
    output_path = os.path.join(OUTPUT_DIR, filename.replace(".jsonl", "_combined.json"))

    print(f"\n📄 Processing: {filename}")

    final_items = []

    # Load all pages
    with open(input_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for line in lines:
        page_data = json.loads(line)
        page_number = page_data["page"]
        text = page_data["text"].strip()

        if not text:
            print(f"➡️ Page {page_number}: EMPTY → []")
            continue

        print(f"➡️ Processing Page {page_number}...")

        llm_output = call_llm(text, page_number)
        items = extract_json(llm_output)

        # Clean values
        for item in items:
            for key in item:
                item[key] = clean_value(item[key])

        final_items.extend(items)

    # SAVE without unicode escapes
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_items, f, indent=2, ensure_ascii=False)

    print("\n✅ Extraction complete!")
    print(f"📦 Saved: {output_path}")


# ======================================================
# RUN
# ======================================================
if __name__ == "__main__":
    process_jsonl_file("Jet2.pdf.jsonl")