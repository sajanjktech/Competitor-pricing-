import os
import fitz  # PyMuPDF
import docx2txt

INPUT_DIR = "input_files"
OUTPUT_DIR = "extracted"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# -------------------------
# PDF extraction (PyMuPDF)
# -------------------------
def extract_pdf(path):
    text = ""
    doc = fitz.open(path)
    for page in doc:
        text += page.get_text("text") + "\n"
    return text


# -------------------------
# DOCX/DOC extraction
# -------------------------
def extract_docx(path):
    return docx2txt.process(path)


# -------------------------
# Extract single file
# -------------------------
def extract_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".pdf":
        return extract_pdf(file_path)
    elif ext in [".docx", ".doc"]:
        return extract_docx(file_path)
    else:
        raise Exception(f"Unsupported file format: {ext}")


# -------------------------
# Process all files
# -------------------------
def process_all_files():
    for filename in os.listdir(INPUT_DIR):
        if filename.lower().endswith((".pdf", ".docx", ".doc")):
            in_path = os.path.join(INPUT_DIR, filename)

            print(f"Extracting: {filename}")

            text = extract_file(in_path)

            # Save to extracted folder
            txt_filename = filename + ".txt"
            out_path = os.path.join(OUTPUT_DIR, txt_filename)

            with open(out_path, "w", encoding="utf-8") as f:
                f.write(text)

            print(f"Saved: {txt_filename}")


if __name__ == "__main__":
    process_all_files()
