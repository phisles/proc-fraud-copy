import os
import json
import re
from collections import defaultdict
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer

PDF_DIRECTORY = "./test"
OUTPUT_DIRECTORY = "./processed_data"
OUTPUT_TEMPLATE_FILE = os.path.join(OUTPUT_DIRECTORY, "template_text.json")  # Ensure correct path

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file and return as a list of normalized sentences."""
    sentences = set()
    for page_layout in extract_pages(pdf_path):
        for element in page_layout:
            if isinstance(element, LTTextContainer):
                text = element.get_text().strip()
                text = clean_text(text)
                sentences.update(re.split(r'(?<=[.!?])\s+', text))  # Split into sentences
    return sentences

def clean_text(text):
    """Normalize text by removing special characters and whitespace."""
    text = text.lower().strip()
    text = re.sub(r'\s+', ' ', text)  # Remove excessive whitespace
    return text

def find_common_text(pdf_files):
    """Identify text that appears in all PDFs."""
    all_text_sets = []
    
    for pdf_file in pdf_files:
        pdf_path = os.path.join(PDF_DIRECTORY, pdf_file)
        text_set = extract_text_from_pdf(pdf_path)
        all_text_sets.append(text_set)

    # Find intersection (text that appears in all PDFs)
    common_text = set.intersection(*all_text_sets) if all_text_sets else set()
    
    return list(common_text)

def main():
    """Run the template text extraction process."""
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)  # Ensure directory exists

    pdf_files = [f for f in os.listdir(PDF_DIRECTORY) if f.endswith(".pdf")]
    
    if len(pdf_files) < 2:
        print("⚠️ Not enough PDFs to detect template text. At least 2 required.")
        return

    common_template_text = find_common_text(pdf_files)

    # Save the template text to a file
    with open(OUTPUT_TEMPLATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"template_text": common_template_text}, f, indent=4)
    
    print(f"✅ Template text extracted and saved to {OUTPUT_TEMPLATE_FILE}")

if __name__ == "__main__":
    main()