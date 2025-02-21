import os
import json
import csv
import re
import time
import multiprocessing
from collections import defaultdict
from difflib import SequenceMatcher
from concurrent.futures import ProcessPoolExecutor

PROCESSED_DIRECTORY = "./processed_data"
MATCHES_REPORT_FILE = "matches_report.csv"
SUMMARY_REPORT_FILE = "summary_report.csv"
TEXT_SIMILARITY_THRESHOLD = 0.75
BATCH_SIZE = 2  # Adjust batch size as needed

def load_processed_files():
    """Load extracted text and image hashes from JSON files, ignoring `template_text.json`."""
    data = {}
    for file in os.listdir(PROCESSED_DIRECTORY):
        if file.endswith(".json") and file != "template_text.json":  # Ignore template_text.json
            with open(os.path.join(PROCESSED_DIRECTORY, file), "r", encoding="utf-8") as f:
                data[file] = json.load(f)
    return data

def get_processed_rows_count():
    """Check the number of rows processed in the matches report CSV."""
    if not os.path.exists(MATCHES_REPORT_FILE):
        return 0  # If the file doesn't exist, start from the beginning
    with open(MATCHES_REPORT_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        return sum(1 for _ in reader) - 1  # Subtract 1 for header row

def clean_text(text):
    """Normalize text by removing extra spaces, special characters, and case differences."""
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)  # Remove excessive whitespace
    text = text.replace('\u00a0', ' ')  # Replace non-breaking spaces
    return text.strip()

def text_similarity(text1, text2):
    """Compute similarity ratio between two texts."""
    return SequenceMatcher(None, text1, text2).ratio()

def extract_matching_sentences(text_by_page1, text_by_page2):
    """Find and return matching sentences along with their actual page locations."""
    matches = []
    max_page1 = max(map(int, text_by_page1.keys()))  # Get the highest page number
    for page1, text1 in text_by_page1.items():
        if int(page1) < 9 or int(page1) > max_page1 - 11:  # Ignore before page 9 and last 11 pages
            continue
        sentences1 = [(page1, s.strip()) for s in re.split(r'(?<=[.!?])\s+', clean_text(text1)) if len(s) > 50]
        max_page2 = max(map(int, text_by_page2.keys()))  # Get the highest page number
        for page2, text2 in text_by_page2.items():
            if int(page2) < 9 or int(page2) > max_page2 - 11:  # Ignore before page 9 and last 11 pages
                continue
            sentences2 = [(page2, s.strip()) for s in re.split(r'(?<=[.!?])\s+', clean_text(text2)) if len(s) > 50]

            for (p1, sent1) in sentences1:
                for (p2, sent2) in sentences2:
                    similarity = text_similarity(sent1, sent2)
                    if similarity >= TEXT_SIMILARITY_THRESHOLD:
                        matches.append((p1, p2, sent1[:200] + "..."))  # Truncate long text
    return matches

def compare_images(image1, image2):
    p1, hash1, file1, pos1 = image1  # Extract values from tuple
    p2, hash2, file2, pos2 = image2  # Extract values from tuple

    print(f"🔍 Checking Image Hash: {hash1} vs {hash2}")  # Debug

    if hash1 == hash2:  # Only compare the hash
        print(f"✅ [MATCH] Image Match on Page {p1} ↔ Page {p2}")
        return (p1, p2, pos1, pos2)  # Return page numbers + positions
    return None  # No match

def process_image_comparison(images1, images2, max_page1, max_page2):
    """Compare images but ignore pages before 9 and last 11 pages."""
    process_id = os.getpid()  # Get process ID
    print(f"⚡ [Process {process_id}] Started image comparison ({len(images1)} vs {len(images2)} images)...")

    start_time = time.time()
    matches = []

    for img1 in images1:
        page1 = img1["page"]
        if page1 < 9 or page1 > max_page1 - 11:
            continue

        for img2 in images2:
            page2 = img2["page"]
            if page2 < 9 or page2 > max_page2 - 11:
                continue

            print(f"🔍 Comparing {img1['image_file']} (Page {page1}) ↔ {img2['image_file']} (Page {page2})")
            result = compare_images((page1, img1["hash"], img1["image_file"], img1["position"]),
                                    (page2, img2["hash"], img2["image_file"], img2["position"]))
            if result:
                print(f"✅ Match found: Page {result[0]} ↔ Page {result[1]}")
                matches.append(result)

    elapsed_time = time.time() - start_time
    print(f"✅ [Process {process_id}] Completed in {elapsed_time:.2f} seconds. {len(matches)} matches found.")
    return matches

def run_image_comparison_task(args):
    """Helper function to unpack arguments for process_image_comparison."""
    file1, file2, images1, images2, max_page1, max_page2 = args
    results = process_image_comparison(images1, images2, max_page1, max_page2)
    return [(file1, file2, *match) for match in results]  # Add filenames to results

def compare_documents_in_batches(data):
    """Compare documents and detect matching text and images in batches."""
    matches = []
    summary_data = defaultdict(lambda: {"matches": [], "text_match": False, "image_match": False})
    files = list(data.keys())

    processed_pairs = set()  # Set to track processed pairs
    processed_rows = get_processed_rows_count()
    print(f"Resuming from row {processed_rows} in {MATCHES_REPORT_FILE}.")

    batch_start = processed_rows
    batch_end = batch_start + BATCH_SIZE
    current_batch = 1
    num_cpus = max(1, os.cpu_count() - 1)

    # Divide work into batches
    for i in range(batch_start, len(files), BATCH_SIZE):
        batch_files = files[i:i+BATCH_SIZE]
        print(f"Processing batch {current_batch} (Files {i+1} to {i+len(batch_files)})")

        image_comparison_tasks = []
        for file1 in batch_files:
            if "text_by_page" not in data[file1]:
                print(f"❌ ERROR: Missing 'text_by_page' in {file1}.")
                continue

            text_by_page1 = data[file1]["text_by_page"]
            images1 = data[file1].get("images", [])
            max_page1 = max(map(int, text_by_page1.keys())) if text_by_page1 else 0

            for file2 in files:
                if file1 == file2:
                    continue  # Skip self-comparison

                # To avoid duplicate comparisons, sort and check if pair has already been processed
                file_pair = tuple(sorted([file1, file2]))
                if file_pair in processed_pairs:
                    continue  # Skip if this pair has already been processed
                processed_pairs.add(file_pair)  # Mark this pair as processed

                if "text_by_page" not in data[file2]:
                    print(f"❌ ERROR: Missing 'text_by_page' in {file2}.")
                    continue

                text_by_page2 = data[file2]["text_by_page"]
                images2 = data[file2].get("images", [])
                max_page2 = max(map(int, text_by_page2.keys())) if text_by_page2 else 0

                matching_sentences = extract_matching_sentences(text_by_page1, text_by_page2)

                if images1 and images2:
                    image_comparison_tasks.append((file1, file2, images1, images2, max_page1, max_page2))

                if matching_sentences:
                    summary_data[file1]["text_match"] = True
                    summary_data[file2]["text_match"] = True

                if images1 and images2:
                    summary_data[file1]["image_match"] = True
                    summary_data[file2]["image_match"] = True

                if matching_sentences:
                    summary_data[file1]["matches"].append(file2)
                    summary_data[file2]["matches"].append(file1)
                    for p1, p2, txt in matching_sentences:
                        matches.append([file1, file2, "Text Match", f"Page {p1}", f"Page {p2}", txt, ""])

        # Execute image comparison for the batch
        if image_comparison_tasks:
            with ProcessPoolExecutor(max_workers=num_cpus) as executor:
                results = list(executor.map(run_image_comparison_task, image_comparison_tasks))

            for result_set in results:
                for file1, file2, p1, p2, pos1, pos2 in result_set:
                    matches.append([file1, file2, "Image Match", f"Page {p1}", f"Page {p2}", pos1, pos2])

        # Save progress after processing each batch
        save_matches_to_csv(matches)  # Save matches to CSV after processing the batch
        save_summary_to_csv(summary_data)

        print(f"✅ Batch {current_batch} complete. {len(matches)} total matches so far.")
        current_batch += 1

    return matches, summary_data

    
def save_matches_to_csv(matches):
    """Save detailed matches to a CSV file with properly structured columns."""
    with open(MATCHES_REPORT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Match Type", "File1", "File2", "File1 Page", "File2 Page", "File1 Position", "File2 Position", "Matched Text"])

        for match in matches:
            match_type = match[2]  # Ensure Match Type is correctly assigned
            file1 = match[0]
            file2 = match[1]
            file1_page = match[3]
            file2_page = match[4]

            if match_type == "Text Match":
                matched_text = match[5]  # Store matched text
                file1_position = ""
                file2_position = ""
            elif match_type == "Image Match":
                file1_position = match[5]
                file2_position = match[6]
                matched_text = ""

            # Write in proper order
            writer.writerow([match_type, file1, file2, file1_page, file2_page, file1_position, file2_position, matched_text])
    
    print(f"✅ Match results saved to {MATCHES_REPORT_FILE}")

def save_summary_to_csv(summary_data):
    """Save summary report to a CSV file."""
    with open(SUMMARY_REPORT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["File", "Matching Files", "Text Match", "Image Match", "Both Match"])
        seen_pairs = set()
        for file, details in summary_data.items():
            for match in details["matches"]:
                pair = tuple(sorted([file, match]))  # Ensure order consistency
                if pair in seen_pairs:
                    continue  # Skip duplicate pairs
                seen_pairs.add(pair)

                writer.writerow([
                    file,
                    match,
                    details["text_match"],
                    details["image_match"],
                    details["text_match"] and details["image_match"]
                ])

    print(f"Summary results saved to {SUMMARY_REPORT_FILE}")

def main():
    """Main function to run the matching process in batches."""
    data = load_processed_files()
    matches, summary_data = compare_documents_in_batches(data)
    print("✅ All batches processed.")

if __name__ == "__main__":
    main()
