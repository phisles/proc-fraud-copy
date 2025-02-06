import os
import json
import csv
import re
from collections import defaultdict
from difflib import SequenceMatcher

PROCESSED_DIRECTORY = "./processed_data"  # Directory containing extracted JSON data
MATCHES_REPORT_FILE = "matches_report.csv"
SUMMARY_REPORT_FILE = "summary_report.csv"
TEXT_SIMILARITY_THRESHOLD = 0.75  # Lower threshold for better detection

def load_processed_files():
    """Load extracted text and image hashes from JSON files."""
    data = {}
    for file in os.listdir(PROCESSED_DIRECTORY):
        if file.endswith(".json"):
            with open(os.path.join(PROCESSED_DIRECTORY, file), "r", encoding="utf-8") as f:
                data[file] = json.load(f)
    return data

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
    for page1, text1 in text_by_page1.items():
        sentences1 = [(page1, s.strip()) for s in re.split(r'(?<=[.!?])\s+', clean_text(text1)) if len(s) > 50]
        for page2, text2 in text_by_page2.items():
            sentences2 = [(page2, s.strip()) for s in re.split(r'(?<=[.!?])\s+', clean_text(text2)) if len(s) > 50]

            for (p1, sent1) in sentences1:
                for (p2, sent2) in sentences2:
                    similarity = text_similarity(sent1, sent2)
                    if similarity >= TEXT_SIMILARITY_THRESHOLD:
                        matches.append((p1, p2, sent1[:200] + "..."))  # Truncate long text
    return matches

def compare_documents(data):
    """Compare documents and detect matching text and images."""
    matches = []
    summary_data = defaultdict(lambda: {"matches": [], "text_match": False, "image_match": False})
    files = list(data.keys())

    for i in range(len(files)):
        file1 = files[i]
        text_by_page1 = data[file1]["text_by_page"]
        images1 = [(img["page"], img["hash"], img["image_file"], img["position"]) for img in data[file1]["image_data"]]

        for j in range(i + 1, len(files)):
            file2 = files[j]
            text_by_page2 = data[file2]["text_by_page"]
            images2 = [(img["page"], img["hash"], img["image_file"], img["position"]) for img in data[file2]["image_data"]]

            print(f"\nComparing {file1} with {file2}")
            matching_sentences = extract_matching_sentences(text_by_page1, text_by_page2)

            # Compare image hashes
            image_matches = []
            for (p1, hash1, img_file1, pos1) in images1:
                for (p2, hash2, img_file2, pos2) in images2:
                    if str(hash1) == str(hash2):  # Ensure correct string comparison
                        print(f"✅ Image match found: {file1} Page {p1} ({img_file1}, {pos1}) ↔ {file2} Page {p2} ({img_file2}, {pos2})")
                        image_matches.append((p1, p2, img_file1, img_file2, pos1, pos2))

            # Update summary report data
            match_type = []
            if matching_sentences:
                match_type.append("Text Match")
                summary_data[file1]["text_match"] = True
                summary_data[file2]["text_match"] = True
            if image_matches:
                match_type.append("Image Match")
                summary_data[file1]["image_match"] = True
                summary_data[file2]["image_match"] = True

            if matching_sentences or image_matches:
                summary_data[file1]["matches"].append(file2)
                summary_data[file2]["matches"].append(file1)

                for p1, p2, txt in matching_sentences:
                    matches.append([file1, file2, "Text Match", f"Page {p1}", f"Page {p2}", txt, ""])
                for p1, p2, img_file1, img_file2, pos1, pos2 in image_matches:
                    matches.append([file1, file2, "Image Match", f"Page {p1}", f"Page {p2}", img_file1, img_file2, pos1, pos2])

    return matches, summary_data

def save_matches_to_csv(matches):
    """Save detailed matches to a CSV file."""
    with open(MATCHES_REPORT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["File 1", "File 2", "Match Type", "File1 Page", "File2 Page", "File1 Image", "File2 Image", "File1 Position", "File2 Position"])
        writer.writerows(matches)
    print(f"Match results saved to {MATCHES_REPORT_FILE}")

def save_summary_to_csv(summary_data):
    """Save summary report to a CSV file."""
    with open(SUMMARY_REPORT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["File", "Matching Files", "Text Match", "Image Match", "Both Match"])
        for file, details in summary_data.items():
            writer.writerow([file, ", ".join(details["matches"]), details["text_match"], details["image_match"], details["text_match"] and details["image_match"]])
    print(f"Summary results saved to {SUMMARY_REPORT_FILE}")

def main():
    """Main function to run the matching process."""
    data = load_processed_files()
    matches, summary_data = compare_documents(data)
    save_matches_to_csv(matches)
    save_summary_to_csv(summary_data)

if __name__ == "__main__":
    main()