import os
import fitz  # PyMuPDF
import docx2txt
from docx import Document
import json

INPUT_DIR = "input_files"
OUTPUT_DIR = "extracted"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ----------------------------------------------
# PDF Extraction - PAGE BY PAGE
# ----------------------------------------------
def extract_pdf_pages(path):
    doc = fitz.open(path)
    pages = []

    for page_number, page in enumerate(doc, start=1):
        text = page.get_text("text") or ""
        pages.append({"page": page_number, "text": text})

    return pages


# ----------------------------------------------
# DOCX Extraction - PAGE SIMULATION
# DOCX has no true pages → split by paragraph length
# ----------------------------------------------
def extract_docx_pages(path, approx_chars_per_page=1800):
    full_text = docx2txt.process(path)

    if not full_text.strip():
        return []

    words = full_text.split()
    pages = []
    current_page_text = []
    current_length = 0
    page_number = 1

    for word in words:
        current_length += len(word) + 1
        current_page_text.append(word)

        if current_length >= approx_chars_per_page:
            pages.append({"page": page_number, "text": " ".join(current_page_text)})
            page_number += 1
            current_page_text = []
            current_length = 0

    # last page
    if current_page_text:
        pages.append({"page": page_number, "text": " ".join(current_page_text)})

    return pages


# ----------------------------------------------
# Extract a single file page-wise
# ----------------------------------------------
def extract_file_pages(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".pdf":
        return extract_pdf_pages(file_path)
    elif ext in [".docx", ".doc"]:
        return extract_docx_pages(file_path)
    else:
        raise Exception(f"Unsupported file format: {ext}")


# ----------------------------------------------
# Process all files → output JSONL per file
# ----------------------------------------------
def process_all_files():
    for filename in os.listdir(INPUT_DIR):
        if filename.lower().endswith((".pdf", ".docx", ".doc")):

            in_path = os.path.join(INPUT_DIR, filename)
            print(f"\n📄 Extracting (page-wise): {filename}")

            pages = extract_file_pages(in_path)

            # Output JSONL path
            out_filename = filename + ".jsonl"
            out_path = os.path.join(OUTPUT_DIR, out_filename)

            with open(out_path, "w", encoding="utf-8") as f:
                for page in pages:
                    f.write(json.dumps(page, ensure_ascii=False) + "\n")

            print(f"✅ Saved page-wise file → {out_filename} ({len(pages)} pages)")


if __name__ == "__main__":
    process_all_files()