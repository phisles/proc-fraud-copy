import os
import json
import csv
from difflib import SequenceMatcher
from itertools import combinations
import random

TEST_MODE = True  # Set to False to process all files

# Paths
OUTPUT_DIRECTORY = "./processed_data"
CSV_OUTPUT_FILE = "./pdf_comparison.csv"

def load_json_files():
    """Load extracted JSON files from the output directory with progress updates."""
    json_data = {}

    # Files that must be included in the test
    required_files = {
        "docNCCDDB4346693bda14bb5a4e3fb581ce077db81588e96fc9b37e809588d55cf81a2a05fe2e5f9.json",
        "docNCCDDB4346693af18c922cec1362fb8e2e78db877f669e72452134db5bdc3fd023049628b7156.json",
        "docNAC01FB7453539a8f94c77b631f7df9f75cf42daf7bb59b46b8d3117b1a7309f022623fb1cb44.json",
        "docNAC01FB74535356434423e0710933c37c87ef29b807bfe7da71dd9f7253130b0903ad3bc99acf.json",
        "docNAC01FB74535350c8f622374c8eb3bb9b562f002f8bb94aa28452943298b38eb8c57f1310c918.json",
        "docNAC01FB7453535d8ccbb60d6d519f5f0cfc272d7e4072642043641fa964ebdf082bbb34b96f8c.json"
    }

    # Get all JSON files in the directory
    all_files = [f for f in os.listdir(OUTPUT_DIRECTORY) if f.endswith(".json")]

    # Ensure required files are present in the directory
    valid_required_files = [f for f in all_files if f in required_files]

    # Select 3 additional random files
    remaining_files = list(set(all_files) - set(valid_required_files))
    random_files = random.sample(remaining_files, min(1, len(remaining_files)))

    # Combine required files and random files
    files = valid_required_files + random_files

    if not files:
        print("⚠️ No JSON files found in the output directory.")
        return json_data

    print(f"📂 Found {len(files)} JSON files for testing. Loading data...")

    for file in files:
        with open(os.path.join(OUTPUT_DIRECTORY, file), "r", encoding="utf-8") as f:
            json_data[file] = json.load(f)

    print(f"✅ Loaded {len(json_data)} JSON files successfully.")
    return json_data

def compute_text_similarity(text1, text2):
    """Compute similarity score between two text contents using SequenceMatcher."""
    return SequenceMatcher(None, text1, text2).ratio() * 100  # Convert to percentage

def compute_image_similarity(images1, images2):
    """Compute image similarity based on matching perceptual hashes."""
    if not images1 or not images2:
        return 0.0  # No images to compare
    
    hashes1 = {img["hash"] for img in images1}
    hashes2 = {img["hash"] for img in images2}
    common_hashes = hashes1.intersection(hashes2)
    total_hashes = len(hashes1.union(hashes2))
    
    return (len(common_hashes) / total_hashes) * 100 if total_hashes > 0 else 0.0

def compare_pdfs(json_data):
    """Compare each pair of PDFs and store similarity scores with live updates."""
    results = []
    total_pairs = sum(1 for _ in combinations(json_data.items(), 2))
    print(f"🔄 Starting PDF comparisons... {total_pairs} total pairs to compare.")

    for idx, ((file1, data1), (file2, data2)) in enumerate(combinations(json_data.items(), 2), start=1):
        # Extract text from pages
        text1 = " ".join(data1.get("text_by_page", {}).values())
        text2 = " ".join(data2.get("text_by_page", {}).values())
        text_similarity = compute_text_similarity(text1, text2)

        # Extract image data
        image_similarity = compute_image_similarity(data1.get("images", []), data2.get("images", []))

        # Compute overall match score (weighted average, tweak as needed)
        overall_match = (0.7 * text_similarity) + (0.3 * image_similarity)

        # Store result
        results.append([file1, file2, round(text_similarity, 2), round(image_similarity, 2), round(overall_match, 2)])

        # Print progress every 10 comparisons
        if idx % 10 == 0 or idx == total_pairs:
            print(f"✅ Processed {idx}/{total_pairs} pairs... Last Match: {file1} <> {file2} - {round(overall_match, 2)}%")
    
    return results

def save_to_csv(results):
    """Save the comparison results to a CSV file with a confirmation message."""
    with open(CSV_OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["PDF_1", "PDF_2", "Text_Similarity (%)", "Image_Similarity (%)", "Overall_Match (%)"])
        writer.writerows(results)
    
    print(f"📄 Results saved to {CSV_OUTPUT_FILE} with {len(results)} comparisons.")

def print_match_statistics(results):
    """Print the top matches and the range of similarity scores."""
    sorted_results = sorted(results, key=lambda x: x[4], reverse=True)  # Sort by Overall_Match (%)
    
    print("\nTop Matching PDFs:")
    for i, (file1, file2, text_sim, img_sim, overall) in enumerate(sorted_results[:5], start=1):
        print(f"{i}. {file1} <> {file2} - Overall Match: {overall}% (Text: {text_sim}%, Images: {img_sim}%)")
    
    if results:
        highest_match = sorted_results[0][4]
        lowest_match = sorted_results[-1][4]
        print(f"\nSimilarity Score Range: Highest - {highest_match}%, Lowest - {lowest_match}%")

def main():
    """Main function to load JSON, compare PDFs, and save results."""
    json_data = load_json_files()
    results = compare_pdfs(json_data)
    save_to_csv(results)
    print_match_statistics(results)
    print(f"✅ Comparison complete. Results saved to {CSV_OUTPUT_FILE}")

if __name__ == "__main__":
    main()
