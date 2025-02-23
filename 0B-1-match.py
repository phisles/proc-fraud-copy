import os
import json
import csv
from difflib import SequenceMatcher
from itertools import combinations

# Paths
OUTPUT_DIRECTORY = "./processed_data"
CSV_OUTPUT_FILE = "./pdf_comparison.csv"

def load_json_files():
    """Load extracted JSON files from the output directory."""
    json_data = {}
    for file in os.listdir(OUTPUT_DIRECTORY):
        if file.endswith(".json"):
            with open(os.path.join(OUTPUT_DIRECTORY, file), "r", encoding="utf-8") as f:
                json_data[file] = json.load(f)
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
    """Compare each pair of PDFs and store similarity scores."""
    results = []
    for (file1, data1), (file2, data2) in combinations(json_data.items(), 2):
        
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
    
    return results

def save_to_csv(results):
    """Save the comparison results to a CSV file."""
    with open(CSV_OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["PDF_1", "PDF_2", "Text_Similarity (%)", "Image_Similarity (%)", "Overall_Match (%)"])
        writer.writerows(results)

def main():
    """Main function to load JSON, compare PDFs, and save results."""
    json_data = load_json_files()
    results = compare_pdfs(json_data)
    save_to_csv(results)
    print(f"✅ Comparison complete. Results saved to {CSV_OUTPUT_FILE}")

if __name__ == "__main__":
    main()
