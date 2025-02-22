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
    """Extract text from a PDF file, preserving paragraph structure and showing debug info."""
    sentences = []
    print(f"\n📄 Extracting text from: {pdf_path}")

    full_text = []  # Store full text chunks before cleaning

    for page_layout in extract_pages(pdf_path):
        for element in page_layout:
            if isinstance(element, LTTextContainer):
                text = element.get_text().strip()
                if text:  
                    full_text.append(text)  # Store full text before cleaning
    
    # Show a preview of raw extracted text before processing
    print(f"   🔎 Raw extracted text preview (first 5 snippets):")
    for snippet in full_text[:5]:  # Show first 5 full lines
        print(f"      - {snippet[:100]}...")  # Print first 100 characters

    # Normalize and preserve ordering
    cleaned_text = [clean_text(t) for t in full_text]

    print(f"   ✅ Cleaned {len(cleaned_text)} text blocks. Showing first 5:")
    for snippet in cleaned_text[:5]:  # Show first 5 cleaned lines
        print(f"      - {snippet[:100]}...")  # Print first 100 characters
    
    return cleaned_text

def clean_text(text):
    """Normalize text by removing special characters and whitespace."""
    text = text.lower().strip()
    text = re.sub(r'\s+', ' ', text)  # Remove excessive whitespace
    return text

def find_common_text(pdf_files):
    """Identify frequently occurring text across PDFs and automatically remove boilerplate."""
    text_count = defaultdict(int)
    total_pdfs = len(pdf_files)

    print("\n📊 Counting text occurrences across PDFs...")

    for pdf_file in pdf_files:
        pdf_path = os.path.join(PDF_DIRECTORY, pdf_file)
        text_set = set(extract_text_from_pdf(pdf_path))  # Avoid duplicates per PDF
        for text in text_set:
            text_count[text] += 1  # Track occurrences across PDFs

    # Determine automatic boilerplate removal threshold
    boilerplate_threshold = int(0.9 * total_pdfs)  # If text appears in 90%+ of PDFs, it's boilerplate
    min_threshold = max(2, int(0.6 * total_pdfs))  # Keep text appearing in at least 60% of PDFs

    # Exclude overly common text (boilerplate)
    filtered_text = [
        text for text, count in text_count.items()
        if min_threshold <= count < boilerplate_threshold
    ]

    # Debug: Show what got filtered
    print("\n🔍 Boilerplate text removed (10 most common phrases filtered out):")
    sorted_text = sorted(text_count.items(), key=lambda x: x[1], reverse=True)[:10]
    for text, count in sorted_text:
        if count >= boilerplate_threshold:
            print(f"   - [{count} PDFs] {text[:100]}...")  # Show first 100 chars of removed boilerplate

    # Debug: Show first 10 retained phrases
    print(f"\n✅ Kept {len(filtered_text)} important text items. Showing first 10:")
    for snippet in filtered_text[:10]:
        print(f"   - {snippet[:100]}...")  # Print first 100 chars

    return filtered_text


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
