import os
import json
import string
from collections import defaultdict
from fuzzywuzzy import fuzz
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer

# === CONFIGURATION ===
PDF_DIRECTORY = "./test"
OUTPUT_DIRECTORY = "./processed_data"
OUTPUT_TEMPLATE_FILE = os.path.join(OUTPUT_DIRECTORY, "template_text.json")

# === CLEAN TEXT FUNCTION ===
def clean_page_text(text):
    """Normalize page text for better comparison."""
    text = text.lower().strip()
    text = "".join([c if c.isalnum() or c.isspace() else " " for c in text])  # Remove special characters
    text = " ".join(text.split())  # Remove extra whitespace
    return text

# === EXTRACT TEXT FROM PDF ===
def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file, preserving page structure."""
    sentences_by_page = []
    print(f"\n📄 Extracting text from: {pdf_path}")

    for page_layout in extract_pages(pdf_path):
        page_text = []
        for element in page_layout:
            if isinstance(element, LTTextContainer):
                text = element.get_text().strip()
                if text:
                    page_text.append(text)

        full_page_text = " ".join(page_text).strip()
        if full_page_text:
            sentences_by_page.append(clean_page_text(full_page_text))

    print(f"   ✅ Extracted {len(sentences_by_page)} pages.")
    return sentences_by_page  # Return cleaned text per page

# === FIND COMMON TEMPLATE TEXT (SMARTER FILTERING) ===
def find_common_text(pdf_files):
    """Identify frequently occurring text across PDFs and remove template portions, keeping unique text."""
    phrase_count = defaultdict(int)
    text_by_pdf = {}
    total_pdfs = len(pdf_files)

    print("\n📊 Counting text occurrences across PDFs...")

    for pdf_index, pdf_file in enumerate(pdf_files):
        pdf_path = os.path.join(PDF_DIRECTORY, pdf_file)
        print(f"   🟢 Processing PDF {pdf_index+1}/{len(pdf_files)}: {pdf_file}")

        pages = extract_text_from_pdf(pdf_path)  # Extract per-page text
        text_by_pdf[pdf_file] = pages

        for page_text in pages:
            sentences = page_text.split(". ")  # Break into sentences
            for sentence in sentences:
                phrase_count[sentence] += 1  # Count occurrences

    # Identify template phrases (appearing in 60%+ of PDFs)
    template_phrases = {
        phrase for phrase, count in phrase_count.items() if count >= int(0.6 * total_pdfs)
    }

    print(f"\n🛑 Identified {len(template_phrases)} repeated template phrases (removed).")

    # Keep only non-template portions of the text
    filtered_text = []
    for pdf_file, pages in text_by_pdf.items():
        for page_text in pages:
            sentences = page_text.split(". ")
            new_text = [sentence for sentence in sentences if sentence not in template_phrases]
            filtered_text.append(". ".join(new_text))  # Reconstruct cleaned text

    # Debug: Show first 10 retained phrases
    print(f"\n✅ Kept {len(filtered_text)} unique text items after smarter filtering. Showing first 10:")
    for snippet in filtered_text[:10]:
        print(f"   - {snippet[:100]}...")  # Print first 100 chars

    return filtered_text

# === MAIN FUNCTION ===
def main():
    """Run the template text extraction process."""
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)  # Ensure output directory exists

    pdf_files = [f for f in os.listdir(PDF_DIRECTORY) if f.endswith(".pdf")]

    if len(pdf_files) < 2:
        print("⚠️ Not enough PDFs to detect template text. At least 2 required.")
        return

    common_template_text = find_common_text(pdf_files)

    # Save the template text to a file
    with open(OUTPUT_TEMPLATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"template_text": common_template_text}, f, indent=4)

    print(f"\n✅ Template text extracted and saved to {OUTPUT_TEMPLATE_FILE}")

# === RUN THE SCRIPT ===
if __name__ == "__main__":
    main()
