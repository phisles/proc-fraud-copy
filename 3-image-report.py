import os
import json
import csv
import re
from datetime import datetime

PROCESSED_DIRECTORY = "./processed_data"
CSV_DIRECTORY = "."  # Where the previous CSVs are stored

# Generate timestamp for the output file
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
CSV_OUTPUT_FILE = f"./image_matches_{timestamp}.csv"

def find_latest_pdf_comparison():
    """Find the most recent pdf_comparison_YYYYMMDD_HHMM.csv file."""
    csv_files = [f for f in os.listdir(CSV_DIRECTORY) if re.match(r'pdf_comparison_\d{8}_\d{4}\.csv$', f)]
    if not csv_files:
        raise FileNotFoundError("No pdf_comparison_YYYYMMDD_HHMM.csv files found.")
    
    latest_file = max(csv_files, key=lambda x: x.split("_")[-1].split(".")[0])  # Sort by timestamp
    print(f"Using latest PDF comparison file: {latest_file}")
    return os.path.join(CSV_DIRECTORY, latest_file)

def load_json_data(file_path):
    """Load JSON data from a given file."""
    if not os.path.exists(file_path):
        print(f"Warning: JSON file {file_path} not found.")
        return []
    
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            return json.load(f).get("images", [])
        except json.JSONDecodeError:
            print(f"Error: Unable to parse JSON file {file_path}")
            return []

def extract_matching_images():
    """Extract matching image information from JSON files and save it to a CSV."""
    csv_file = find_latest_pdf_comparison()
    print(f"Reading PDF comparison data from {csv_file}")

    with open(csv_file, "r", newline="", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        header = next(reader)
        rows = list(reader)

    matching_images = []

    for row in rows:
        pdf_1 = row[0]  # First PDF
        pdf_2 = row[1]  # Second PDF

        print(f"Processing matching PDFs: {pdf_1} & {pdf_2}")

        # Load image data from corresponding JSON files
        json_file_1 = os.path.join(PROCESSED_DIRECTORY, pdf_1)
        json_file_2 = os.path.join(PROCESSED_DIRECTORY, pdf_2)

        images_1 = load_json_data(json_file_1)
        images_2 = load_json_data(json_file_2)

        # Create a dictionary of hashes for quick lookup
        image_hashes_1 = {img["hash"]: img for img in images_1}
        image_hashes_2 = {img["hash"]: img for img in images_2}

        # Find matching image hashes
        common_hashes = set(image_hashes_1.keys()) & set(image_hashes_2.keys())

        for img_hash in common_hashes:
            img_data_1 = image_hashes_1[img_hash]
            img_data_2 = image_hashes_2[img_hash]

            matching_images.append([
                pdf_1, pdf_2,
                img_data_1["page"], img_data_1["position"],
                img_data_2["page"], img_data_2["position"]
            ])

    if not matching_images:
        print("No matching images found. Exiting.")
        return

    # Write results to the output CSV
    print(f"Writing matching images to {CSV_OUTPUT_FILE}")
    with open(CSV_OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["PDF1", "PDF2", "PDF1_PageNum", "PDF1_Position", "PDF2_PageNum", "PDF2_Position"])
        writer.writerows(matching_images)

    print(f"Matching images saved to {CSV_OUTPUT_FILE}")

if __name__ == "__main__":
    extract_matching_images()
