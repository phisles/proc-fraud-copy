import os
import json
import string
import re
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
    """Identify frequently occurring text across PDFs, remove headers/footers separately, and preserve unique content."""
    phrase_count = defaultdict(int)
    text_by_pdf = {}
    total_pdfs = len(pdf_files)

    print("\n📊 Counting text occurrences across PDFs...")

    # === STEP 1: Extract Text & Automatically Flag First/Last Sentences as Template ===
    auto_template_phrases = set()  # Stores first/last sentences of pages/paragraphs

    for pdf_index, pdf_file in enumerate(pdf_files):
        pdf_path = os.path.join(PDF_DIRECTORY, pdf_file)
        print(f"   🟢 Processing PDF {pdf_index+1}/{len(pdf_files)}: {pdf_file}")

        pages = extract_text_from_pdf(pdf_path)  # Extract per-page text
        text_by_pdf[pdf_file] = pages

        for page_text in pages:
            # Split text into paragraphs based on newlines
            paragraphs = [p.strip() for p in re.split(r"\n{2,}", page_text) if p.strip()]
            
            # Flag first and last sentence of each page
            sentences = [s.strip() for s in re.split(r"\.\s+|\n", page_text) if s.strip()]
            if len(sentences) > 2:
                auto_template_phrases.add(sentences[0])  # First sentence of page
                auto_template_phrases.add(sentences[-1])  # Last sentence of page

            for paragraph in paragraphs:
                para_sentences = [s.strip() for s in re.split(r"\.\s+|\n", paragraph) if s.strip()]
                if len(para_sentences) > 2:
                    auto_template_phrases.add(para_sentences[0])  # First sentence of paragraph
                    auto_template_phrases.add(para_sentences[-1])  # Last sentence of paragraph

            # Count phrase frequency (for later template detection)
            words = page_text.split()
            for i in range(len(words) - 5):  # Look for 5-word phrases
                phrase = " ".join(words[i : i + 5])
                phrase_count[phrase] += 1  

    # === STEP 2: Identify Common Template Phrases (60%+ of PDFs) ===
    phrase_threshold = int(0.6 * total_pdfs)
    common_template_phrases = {
        phrase for phrase, count in phrase_count.items() if count >= phrase_threshold
    }

    print(f"\n🛑 Identified {len(auto_template_phrases)} auto-detected headers/footers.")
    print(f"🛑 Identified {len(common_template_phrases)} repeated phrases (60%+ PDFs).")

    # === STEP 3: Remove Only Template Content, Keeping Unique Middle Content ===
    filtered_text = []
    for pdf_file, pages in text_by_pdf.items():
        for page_text in pages:
            sentences = [s.strip() for s in re.split(r"\.\s+|\n", page_text) if s.strip()]
            
            # Remove template sentences while keeping unique middle text
            cleaned_sentences = [
                s for s in sentences if s not in auto_template_phrases and s not in common_template_phrases
            ]

            if cleaned_sentences:
                filtered_text.append(". ".join(cleaned_sentences))  # Reconstruct cleaned text

    # Debug: Show first 10 retained phrases
    print(f"\n✅ Kept {len(filtered_text)} unique text items after smart filtering. Showing first 10:")
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
