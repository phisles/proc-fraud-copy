from collections import defaultdict
import string
from fuzzywuzzy import fuzz  # Install using: pip install fuzzywuzzy
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer

# Normalize text for better comparison
def clean_page_text(text):
    """Normalize page text for better comparison."""
    text = text.lower().strip()
    text = "".join([c if c.isalnum() or c.isspace() else " " for c in text])  # Remove special characters
    text = " ".join(text.split())  # Remove extra whitespace
    return text

# Extract text page-by-page
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

# Identify template pages and filter text
def find_common_text(pdf_files):
    """Identify frequently occurring text across PDFs and remove template pages."""
    page_occurrences = defaultdict(list)  # Track which PDFs contain similar pages
    text_count = defaultdict(int)  # Track text occurrences
    total_pdfs = len(pdf_files)
    text_by_pdf = {}

    print("\n📊 Counting text occurrences across PDFs...")

    for pdf_file in pdf_files:
        pdf_path = os.path.join(PDF_DIRECTORY, pdf_file)
        pages = extract_text_from_pdf(pdf_path)  # Extract per-page text
        text_by_pdf[pdf_file] = pages

        for page_idx, page_text in enumerate(pages):
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
    template_pages = {
        text for text, files in page_occurrences.items() if len(files) >= int(0.6 * total_pdfs)
    }
    print(f"\n🛑 Identified {len(template_pages)} repeated template pages (removed).")

    # Keep only text from unique pages
    filtered_text = []
    for pdf_file, pages in text_by_pdf.items():
        for page_text in pages:
            if page_text not in template_pages:  # Remove template pages
                filtered_text.extend(page_text.split("\n"))  # Keep only unique text

    # Remove repeated boilerplate text from remaining pages
    filtered_text = filter_boilerplate_text(filtered_text, text_count, total_pdfs)

    return filtered_text

# Boilerplate text filtering (after removing template pages)
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
