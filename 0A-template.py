import os
import json
import string
from collections import defaultdict
from rapidfuzz import fuzz
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer

# === CONFIGURATION ===
PDF_DIRECTORY = "./test"
OUTPUT_DIRECTORY = "./processed_data"
OUTPUT_TEMPLATE_FILE = os.path.join(OUTPUT_DIRECTORY, "template_text.json")

# === TEXT NORMALIZATION ===
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

# === FIND COMMON TEMPLATE TEXT ===
def find_common_text(pdf_files):
    """Identify frequently occurring text across PDFs and remove template pages."""
    page_occurrences = defaultdict(list)  # Track which PDFs contain similar pages
    text_count = defaultdict(int)  # Track text occurrences
    total_pdfs = len(pdf_files)
    text_by_pdf = {}

    print("\n📊 Counting text occurrences across PDFs...")

    for pdf_index, pdf_file in enumerate(pdf_files):
        pdf_path = os.path.join(PDF_DIRECTORY, pdf_file)
        print(f"   🟢 Processing PDF {pdf_index+1}/{len(pdf_files)}: {pdf_file}")

        pages = extract_text_from_pdf(pdf_path)  # Extract per-page text
        text_by_pdf[pdf_file] = pages

        for page_text in pages:
            for existing_text in page_occurrences:
                similarity = fuzz.ratio(page_text, existing_text)  # Compare similarity
                if similarity > 85:  # If 85% similar, count as the same page
                    page_occurrences[existing_text].append(pdf_file)
                    break
            else:
                page_occurrences[page_text].append(pdf_file)  # New unique page

            # Track text occurrences within the page
            for line in page_text.split("\n"):
                text_count[line] += 1

    # Identify template pages (appearing in 60%+ of PDFs)
    template_pages = {text for text, files in page_occurrences.items() if len(files) >= int(0.6 * total_pdfs)}
    print(f"\n🛑 Identified {len(template_pages)} repeated template pages (removed).")

    # Keep only text from unique pages
    filtered_text = []
    for pdf_file, pages in text_by_pdf.items():
        for page_text in pages:
            if page_text not in template_pages:  # Remove template pages
                filtered_text.extend(page_text.split("\n"))  # Keep only unique text

    # Apply secondary filtering on remaining text
    filtered_text = filter_boilerplate_text(filtered_text, text_count, total_pdfs)

    return filtered_text

# === FILTER BOILERPLATE TEXT ===
def filter_boilerplate_text(filtered_text, text_count, total_pdfs):
    """Removes common boilerplate text after filtering template pages."""
    common_template_words = {
        "yes", "no", "n/a", "true", "false", "description", "duns", "cage", "ueid",
        "firm name", "proposal number", "topic number", "participate", "disclaimer"
    }

    def is_probable_boilerplate(text):
        """Detects if text is likely boilerplate using multiple conditions."""
        if len(text) <= 3:  # Remove very short words/numbers (e.g., "6", "•")
            return True
        if text.lower() in common_template_words:  # Generic template metadata fields
            return True
        if sum(c in string.punctuation for c in text) > 5:  # Too many symbols
            return True
        if text.count("[") >= 2 or text.count("]") >= 2:  # Checkbox-style template items
            return True
        if text[0].isdigit() and "." in text[:5]:  # Lines that start with "15. ..."
            return True
        return False

    # Remove highly common text (appears in 60%+ of PDFs)
    boilerplate_threshold = int(0.6 * total_pdfs)
    filtered_text = [
        text for text in filtered_text
        if text_count[text] < boilerplate_threshold and not is_probable_boilerplate(text)
    ]

    # Debug: Show first 10 retained phrases
    print(f"\n✅ Kept {len(filtered_text)} unique text items after full filtering. Showing first 10:")
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
