import os
import json
import imagehash
import fitz  # PyMuPDF for extracting images
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer
from PIL import Image
import io
from collections import defaultdict
import re
from difflib import SequenceMatcher

# Set paths
PDF_DIRECTORY = "./test"  # Directory containing PDFs
OUTPUT_DIRECTORY = "./processed_data"  # Where extracted text and images will be saved
IMAGE_OUTPUT_DIRECTORY = os.path.join(OUTPUT_DIRECTORY, "images")

# Ensure output directories exist
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
os.makedirs(IMAGE_OUTPUT_DIRECTORY, exist_ok=True)

# Track image occurrences to filter duplicate images
image_counts = defaultdict(int)

def get_ngrams(text, n=5):
    words = text.split()
    if len(words) < n:
        return set()
    return {" ".join(words[i:i+n]) for i in range(len(words)-n+1)}

def clean_text(text):
    """Normalize text by removing special characters and extra spaces."""
    text = text.lower().strip()
    text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces with a single space
    text = re.sub(r'[^\w\s.,!?-]', '', text)  # Remove non-alphanumeric characters except punctuation
    return text


import re

import re

def extract_firm_info(json_data):
    """Extracts firm and contact information from JSON instead of reprocessing the PDF."""
    firm_info = {}

    # Ensure required pages exist in JSON
    text_by_page = json_data.get("text_by_page", {})
    page_2_text = text_by_page.get("2", "").lower()
    # Force string key and debug print
    page_7_text = text_by_page.get(str(7), "").strip().lower()

    # Debugging: Show exactly what is being retrieved
    print(f"\n🔍 DEBUG: Extracted Text from Page 7 (Full JSON Value): {repr(page_7_text)}\n")

    # If text is empty, print available keys
    if not page_7_text:
        print(f"⚠️ Warning: Page 7 text is missing or empty. Available keys: {list(text_by_page.keys())}")

    # Debug: Show actual content including special characters
    print("\n🔍 DEBUG: Extracted Text from Page 7 (Raw):", repr(page_7_text))

    # Check if text is non-empty after removing excessive spaces
    if not re.sub(r'\s+', '', page_7_text):  # Remove all whitespace and check if anything remains
        print(f"⚠️ Warning: Page 7 text not found or is empty. Available keys: {list(text_by_page.keys())}")

    print("\n🔍 DEBUG: Extracted Text from Page 7 (Raw):", repr(page_7_text))  # Show exact content

    print("\n🔍 DEBUG: Extracted Text from Page 7:\n", page_7_text)

    if not page_7_text:
        print("⚠️ Warning: Page 7 text not found in JSON.")
        return None

    # Extract and format company name
    firm_name_match = re.search(r"firm name\s+([\w\s.,&-]+?)\s+address", page_2_text, re.IGNORECASE)
    firm_name = firm_name_match.group(1).strip().title() if firm_name_match else "N/A"
    print(f"✅ Company: {firm_name}")
    firm_info["company"] = firm_name

    # Extract and format address
    print("\n🔍 DEBUG: Searching for 'address' in Page 7...\n")

    address_match = re.search(r"address\s+(.+?)(?=\s+(corporate official name|phone|email))", page_7_text, re.IGNORECASE)
    if address_match:
        address = address_match.group(1).strip()
        address = address.title()  # Capitalize each word
        address = re.sub(r"\b([a-z]{2})\b", lambda x: x.group(1).upper(), address)  # Capitalize state
        print(f"✅ Address: {address}")
    else:
        print("❌ DEBUG: Address Extraction Failed.")
        address = "N/A"

    firm_info["address"] = address

    # **Fixing Website Extraction**
    print("\n🔍 DEBUG: Searching for 'website' in Page 2...\n")

    # Try to extract a website (anything that looks like a domain name)
    website_match = re.search(r"cage\s+\S+\s+[\d\s,-]+[a-z\s]+?\d{5}(?:-\d{4})?\s+(\S+\.\S+)", page_2_text, re.IGNORECASE)

    if website_match:
        website = website_match.group(1).strip()
    else:
        # Fallback: Find a proper domain anywhere in Page 2
        possible_websites = re.findall(r"([a-z0-9.-]+\.[a-z]{2,})", page_2_text)
        website = possible_websites[0].strip() if possible_websites else "N/A"

    # **CLEANING: Remove unwanted prefixes**
    website = re.sub(r"^(https?:\/\/|httpwww\.|www\.)", "", website).strip()

    print(f"✅ Website: {website}")
    firm_info["website"] = website

    # Extract and format name (was corporate_official_name)
    name_match = re.search(r"name\s+([\w\s.,-]+?)\s+phone", page_7_text, re.IGNORECASE)
    name = name_match.group(1).strip() if name_match else "N/A"
    name = re.sub(r"\s+", " ", name)  # Remove extra spaces and line breaks
    name = name.title()  # Capitalize name correctly
    print(f"✅ Name: {name}")
    firm_info["name"] = name

    # Extract and format phone number
    phone_match = re.search(r"phone\s+([\d\s-]+)\s+email", page_7_text, re.IGNORECASE)
    phone = phone_match.group(1).strip() if phone_match else "N/A"
    print(f"✅ Phone: {phone}")
    firm_info["phone"] = phone

    return firm_info if firm_info else None

def extract_text_from_pdf(pdf_path, template_text):
    """Extract text from a PDF file, capturing the first 9 and last 10-11 pages for contact details."""
    text_by_page = {}
    contact_text_pages = {}

    pdf_document = fitz.open(pdf_path)  # Open the PDF
    total_pages = len(pdf_document)

    # Define pages to extract for contact info
    contact_pages = list(range(9)) + list(range(max(0, total_pages - 11), total_pages))

    normalized_templates = {clean_text(s) for s in template_text if s.strip()}

    for page_number, page_layout in enumerate(extract_pages(pdf_path), start=1):
        text = "".join(
            element.get_text() for element in page_layout if isinstance(element, LTTextContainer)
        ).strip()
        
        if text:
            cleaned_text = clean_text(text)
            sentences = [s.strip() for s in re.split(r'(?<=[.!?])\s+', cleaned_text) if s.strip()]
            filtered_sentences = []

            for sentence in sentences:
                sentence_clean = clean_text(sentence)
                sentence_ngrams = get_ngrams(sentence_clean, 5)
                skip = False

                for templ in normalized_templates:
                    templ_ngrams = get_ngrams(templ, 5)
                    if templ_ngrams and len(sentence_ngrams.intersection(templ_ngrams)) / len(templ_ngrams) > 0.7:
                        skip = True
                        break

                if not skip:
                    filtered_sentences.append(sentence)

            if filtered_sentences:
                text_by_page[page_number] = "\n".join(filtered_sentences)

                # Capture first 9 and last 10-11 pages for contact info
                if page_number - 1 in contact_pages:
                    contact_text_pages[page_number] = "\n".join(filtered_sentences)

    pdf_document.close()
    return text_by_page, contact_text_pages

def get_image_position(bbox, page_width, page_height):
    """Determine the location of an image on the page based on its bounding box."""
    if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
        return "Unknown (Invalid Bounding Box)"
    
    x0, y0, x1, y1 = bbox
    img_center_x = (x0 + x1) / 2
    img_center_y = (y0 + y1) / 2

    # Define grid areas (split into thirds)
    width_third = page_width / 3
    height_third = page_height / 3

    # Determine horizontal position
    if img_center_x < width_third:
        horizontal = "Left"
    elif img_center_x < 2 * width_third:
        horizontal = "Center"
    else:
        horizontal = "Right"

    # Determine vertical position
    if img_center_y < height_third:
        vertical = "Top"
    elif img_center_y < 2 * height_third:
        vertical = "Middle"
    else:
        vertical = "Bottom"

    return f"{vertical} {horizontal}"

def extract_images_from_pdf(pdf_path, pdf_filename):
    """Extract embedded images from a PDF file and return perceptual hashes with position info."""
    image_data = []
    pdf_document = fitz.open(pdf_path)  # Open PDF with PyMuPDF
    
    for page_number in range(len(pdf_document)):
        page = pdf_document[page_number]
        images = page.get_images(full=True)
        page_width, page_height = page.rect.width, page.rect.height  # Get page dimensions

        for img_index, img in enumerate(images):
            xref = img[0]  # Get image reference number
            base_image = pdf_document.extract_image(xref)
            img_bytes = base_image["image"]
            img_ext = base_image["ext"]

            # Convert image to PIL format
            image = Image.open(io.BytesIO(img_bytes))

            # Skip small images (e.g., single-pixel elements, small icons)
            if image.width < 20 or image.height < 20:
                print(f"⚠️ Skipping tiny image on Page {page_number+1}: {image.width}x{image.height}")
                continue

            # Compute perceptual hash
            img_hash = str(imagehash.phash(image))

            # Limit duplicate images appearing too frequently
            image_counts[img_hash] += 1
            if image_counts[img_hash] > 3:  # Skip if the same image appears more than 3 times
                print(f"⚠️ Skipping repeated image (Hash: {img_hash}) on Page {page_number+1}")
                continue

            # Save extracted image
            img_filename = f"{pdf_filename}_page{page_number+1}_img{img_index+1}.{img_ext}"
            img_path = os.path.join(IMAGE_OUTPUT_DIRECTORY, img_filename)
            image.save(img_path)

            # Extract image bounding box (position)
            try:
                img_rects = page.get_image_rects(xref)  # Get bounding box from PyMuPDF
                if img_rects:
                    bbox = img_rects[0]  # Take the first bounding box (if multiple exist)
                    position = get_image_position((bbox.x0, bbox.y0, bbox.x1, bbox.y1), page_width, page_height)
                else:
                    position = "Unknown (No Bounding Box)"
            except Exception as e:
                print(f"⚠️ Warning: Unable to get bounding box for image {img_index+1} on Page {page_number+1} in {pdf_filename}: {e}")
                position = "Unknown (Error)"

            # Store image info
            image_data.append({
                "page": page_number + 1,
                "image_file": img_filename,
                "hash": img_hash,
                "position": position  # Add position info
            })
    
    pdf_document.close()
    return image_data

def load_template_text():
    """Load template text from the JSON file."""
    template_text_file = "./processed_data/template_text.json"
    if os.path.exists(template_text_file):
        with open(template_text_file, "r", encoding="utf-8") as f:
            return set(json.load(f).get("template_text", []))
    return set()

def process_pdf(pdf_file):
    """Process a single PDF file to extract firm information, images, and structured contact details."""
    template_text = load_template_text()
    
    pdf_path = os.path.join(PDF_DIRECTORY, pdf_file)
    pdf_filename = os.path.splitext(pdf_file)[0]
    json_path = os.path.join(OUTPUT_DIRECTORY, pdf_filename + ".json")

    # Extract text from PDF
    extracted_text, _ = extract_text_from_pdf(pdf_path, template_text)
    
    # Extract images
    extracted_images = extract_images_from_pdf(pdf_path, pdf_filename)

    # Prepare base JSON structure
    result = {
        "filename": pdf_file,
        "text_by_page": extracted_text,   # Store full text
        "images": extracted_images        # Store extracted images
    }

    # ✅ First, save extracted text & images to JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4)

    # ✅ Now, reload the updated JSON
    with open(json_path, "r", encoding="utf-8") as f:
        json_data = json.load(f)

    # ✅ Extract firm info using the updated JSON
    firm_info = extract_firm_info(json_data)
    result["firm_info"] = firm_info

    # ✅ Save final JSON file with firm info
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4)

    print(f"✅ Processed {pdf_file}: Extracted firm information and images.")

def main():
    """Main function to process all PDFs in the directory."""
    for pdf_file in os.listdir(PDF_DIRECTORY):
        if pdf_file.lower().endswith(".pdf"):
            process_pdf(pdf_file)

if __name__ == "__main__":
    main()