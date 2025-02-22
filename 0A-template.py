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
    """Identify frequently occurring text across PDFs, remove headers/footers separately, and preserve unique content."""
    phrase_count = defaultdict(int)
    header_footer_count = defaultdict(int)
    text_by_pdf = {}
    total_pdfs = len(pdf_files)

    print("\n📊 Counting text occurrences across PDFs...")

    # === STEP 1: EXTRACT TEXT & DETECT COMMON HEADERS/FOOTERS ===
    for pdf_index, pdf_file in enumerate(pdf_files):
        pdf_path = os.path.join(PDF_DIRECTORY, pdf_file)
        print(f"   🟢 Processing PDF {pdf_index+1}/{len(pdf_files)}: {pdf_file}")

        pages = extract_text_from_pdf(pdf_path)  # Extract per-page text
        text_by_pdf[pdf_file] = pages

        for page_text in pages:
            # Split by both `. ` (dot + space) and `\n` (newlines) to catch headers/footers
            sentences = [s.strip() for s in re.split(r"\.\s+|\n", page_text) if s.strip()]

            # Detect headers & footers (first and last sentences)
            if len(sentences) > 2:
                header_footer_count[sentences[0]] += 1  # First sentence (header)
                header_footer_count[sentences[-1]] += 1  # Last sentence (footer)

            # Count all text for later phrase-based filtering
            for sentence in sentences[1:-1]:  # Ignore first and last (header/footer)
                words = sentence.split()
                for i in range(len(words) - 5):  # Look for 5-word phrases
                    phrase = " ".join(words[i : i + 5])
                    phrase_count[phrase] += 1  

    # === STEP 2: IDENTIFY COMMON HEADERS/FOOTERS ===
    header_footer_threshold = int(0.5 * total_pdfs)  # Lowered to 50%
    template_headers_footers = {
        phrase for phrase, count in header_footer_count.items() if count >= header_footer_threshold
    }

    print(f"\n🛑 Identified {len(template_headers_footers)} repeated headers/footers (REMOVED).")

    # === STEP 3: IDENTIFY COMMON TEMPLATE PHRASES (EXCLUDING HEADERS/FOOTERS) ===
    phrase_threshold = int(0.5 * total_pdfs)  # Lowered to 50%
    template_phrases = {
        phrase for phrase, count in phrase_count.items() if count >= phrase_threshold
    }

    print(f"\n🛑 Identified {len(template_phrases)} repeated short phrases (REMOVED).")

    # === STEP 4: KEEP ONLY NON-TEMPLATE TEXT (AFTER HEADER/FOOTER REMOVAL) ===
    filtered_text = []
    for pdf_file, pages in text_by_pdf.items():
        for page_text in pages:
            sentences = [s.strip() for s in re.split(r"\.\s+|\n", page_text) if s.strip()]

            # Step 4.1: Remove headers/footers
            if len(sentences) > 2:
                core_sentences = sentences[1:-1]  # Keep only middle content
            else:
                core_sentences = sentences  # If only 1-2 sentences, keep all

            # Step 4.2: Remove template phrases inside the middle content
            cleaned_sentences = []
            for sentence in core_sentences:
                words = sentence.split()
                new_sentence = []
                i = 0
                while i < len(words) - 5:
                    phrase = " ".join(words[i : i + 5])
                    if phrase in template_phrases:
                        i += 5  # Skip template phrase
                    else:
                        new_sentence.append(words[i])
                        i += 1
                cleaned_sentences.append(" ".join(new_sentence))

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
