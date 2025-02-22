import os
import re
import json
from collections import defaultdict
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer

# Directory paths
PDF_DIRECTORY = "./test"
OUTPUT_DIRECTORY = "./processed_data"
OUTPUT_TEMPLATE_FILE = os.path.join(OUTPUT_DIRECTORY, "template_text.json")

def clean_page_text(text):
    """Normalize page text while preserving punctuation and sentence structure."""
    text = text.lower().strip()
    
    # Preserve punctuation but remove unnecessary symbols
    text = re.sub(r"[^\w\s.,!?;:]", "", text)  # Keep .,!?;: but remove other special chars
    
    # Ensure proper spacing after punctuation
    text = re.sub(r"([.,!?;:])([^\s])", r"\1 \2", text)  # Add space after punctuation if missing
    
    # Reduce excess spaces
    text = re.sub(r"\s+", " ", text).strip()
    
    return text

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file while preserving page structure."""
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
            full_page_text = clean_page_text(full_page_text)  # Clean text properly
            sentences_by_page.append(full_page_text)

    print(f"   ✅ Extracted {len(sentences_by_page)} pages.")
    return sentences_by_page  # Return cleaned text per page

def find_common_text(pdf_files):
    """Identify frequently occurring text across PDFs while preserving structure."""
    phrase_count = defaultdict(int)
    header_footer_count = defaultdict(int)
    text_by_pdf = {}
    total_pdfs = len(pdf_files)

    print("\n📊 Counting text occurrences across PDFs...")

    possible_headers_footers = set()

    for pdf_index, pdf_file in enumerate(pdf_files):
        pdf_path = os.path.join(PDF_DIRECTORY, pdf_file)
        print(f"   🟢 Processing PDF {pdf_index+1}/{len(pdf_files)}: {pdf_file}")

        pages = extract_text_from_pdf(pdf_path)  # Extract per-page text
        text_by_pdf[pdf_file] = pages

        for page_text in pages:
            # Preserve punctuation so sentences remain intact
            page_text = clean_page_text(page_text)
            
            # Split into properly structured sentences
            sentences = [s.strip() for s in re.split(r"(?<=[.!?;:])\s+", page_text) if s.strip()]
            
            # Store possible headers/footers (only if they appear in 50%+ PDFs)
            if len(sentences) > 2:
                header_footer_count[sentences[0]] += 1  # First sentence (header)
                header_footer_count[sentences[-1]] += 1  # Last sentence (footer)

            # Count phrase frequency (for template detection)
            words = page_text.split()
            for i in range(len(words) - 6):  # Sliding window of 6 words
                phrase = " ".join(words[i : i + 6])
                phrase_count[phrase] += 1  

    # === STEP 2: Identify Common Template Phrases (70%+ PDFs) ===
    header_footer_threshold = int(0.5 * total_pdfs)  # Must appear in 50%+ of PDFs
    phrase_threshold = int(0.7 * total_pdfs)  # Must appear in 70%+ of PDFs

    detected_headers_footers = {
        phrase for phrase, count in header_footer_count.items() if count >= header_footer_threshold
    }

    detected_template_phrases = {
        phrase for phrase, count in phrase_count.items() if count >= phrase_threshold
    }

    print(f"\n🛑 Identified {len(detected_headers_footers)} repeated headers/footers.")
    print(f"🛑 Identified {len(detected_template_phrases)} repeated short phrases (70%+ PDFs).")

    # === STEP 3: Remove Only Template Parts, Not Whole Sentences ===
    filtered_text = []
    for pdf_file, pages in text_by_pdf.items():
        for page_text in pages:
            page_text = clean_page_text(page_text)  # Ensure cleaned version is used
            sentences = [s.strip() for s in re.split(r"(?<=[.!?;:])\s+", page_text) if s.strip()]

            # **Remove First and Last Sentence of Every Paragraph**
            if len(sentences) > 2:
                sentences = sentences[1:-1]  # Remove first and last sentence

            cleaned_sentences = []
            for sentence in sentences:
                words = sentence.split()

                # Step 3.1: If the sentence starts with a detected template phrase, remove only the first few words
                for template_phrase in detected_template_phrases:
                    template_words = template_phrase.split()
                    if words[: len(template_words)] == template_words:  # If sentence starts with a template phrase
                        words = words[len(template_words) :]  # Remove only the prefix
                        break  # Stop checking once template prefix is removed

                cleaned_sentences.append(" ".join(words))  # Keep the remaining content

            if cleaned_sentences:
                filtered_text.append(". ".join(cleaned_sentences))  # Reconstruct cleaned text

    # Debug: Show first 10 retained phrases
    print(f"\n✅ Kept {len(filtered_text)} unique text items after smart filtering. Showing first 10:")
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
