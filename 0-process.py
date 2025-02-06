import os
import json
import imagehash
import fitz  # PyMuPDF for extracting images
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer
from PIL import Image
import io
from collections import defaultdict

# Set paths
PDF_DIRECTORY = "./test"  # Directory containing PDFs
OUTPUT_DIRECTORY = "./processed_data"  # Where extracted text and images will be saved
IMAGE_OUTPUT_DIRECTORY = os.path.join(OUTPUT_DIRECTORY, "images")

# Ensure output directories exist
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
os.makedirs(IMAGE_OUTPUT_DIRECTORY, exist_ok=True)

# Track image occurrences to filter duplicate images
image_counts = defaultdict(int)

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file and preserve page numbers."""
    text_by_page = {}
    for page_number, page_layout in enumerate(extract_pages(pdf_path), start=1):
        text = "".join(element.get_text() for element in page_layout if isinstance(element, LTTextContainer)).strip()
        if text:
            text_by_page[page_number] = text
    return text_by_page

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

def process_pdf(pdf_file):
    """Process a single PDF file for text and image extraction."""
    pdf_path = os.path.join(PDF_DIRECTORY, pdf_file)
    pdf_filename = os.path.splitext(pdf_file)[0]
    
    extracted_text = extract_text_from_pdf(pdf_path)
    extracted_images = extract_images_from_pdf(pdf_path, pdf_filename)
    
    # Save structured results
    result = {
        "filename": pdf_file,
        "text_by_page": extracted_text,
        "image_data": extracted_images
    }
    
    output_file = os.path.join(OUTPUT_DIRECTORY, pdf_filename + ".json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4)
    
    print(f"✅ Processed {pdf_file}: Extracted text & {len(extracted_images)} images.")

def main():
    """Main function to process all PDFs in the directory."""
    for pdf_file in os.listdir(PDF_DIRECTORY):
        if pdf_file.lower().endswith(".pdf"):
            process_pdf(pdf_file)

if __name__ == "__main__":
    main()