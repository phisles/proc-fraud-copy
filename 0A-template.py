import re
import os
import json
from collections import defaultdict
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer

def clean_page_text(text):
    """Normalize page text while keeping sentence structure intact."""
    text = text.lower().strip()
    
    # Preserve key punctuation while removing unwanted symbols
    text = re.sub(r"[^\w\s.,!?;:]", "", text)  # Removes only special symbols but keeps periods, commas, etc.
    
    # Ensure proper spacing after punctuation
    text = re.sub(r"([.,!?;:])([^\s])", r"\1 \2", text)  # Adds space after punctuation if missing
    
    # Reduce excess spaces
    text = re.sub(r"\s+", " ", text).strip()
    
    return text

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

            # Count phrase frequency (for later template detection)
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
