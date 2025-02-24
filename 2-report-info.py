import os
import json
import csv
import re
from datetime import datetime

PROCESSED_DIRECTORY = "./processed_data"
CSV_DIRECTORY = "."  # Assuming CSVs are stored in the current directory

# Generate timestamp for the output file
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
CSV_OUTPUT_FILE = f"./pdf_comparison_{timestamp}.csv"

def find_latest_pdf_comparison():
    """Find the most recent pdf_comparison_YYYYMMDD_HHMM.csv file."""
    csv_files = [f for f in os.listdir(CSV_DIRECTORY) if re.match(r'pdf_comparison_\d{8}_\d{4}\.csv$', f)]
    if not csv_files:
        raise FileNotFoundError("No pdf_comparison_YYYYMMDD_HHMM.csv files found.")
    
    latest_file = max(csv_files, key=lambda x: x.split("_")[-1].split(".")[0])  # Sort by timestamp
    print(f"[INFO] Using latest PDF comparison file: {latest_file}")
    return os.path.join(CSV_DIRECTORY, latest_file)

def sanitize_csv_text(text):
    """
    Sanitize text to prevent CSV format issues:
    - Remove excessive whitespace
    - Replace newlines with spaces
    - Ensure commas, quotes, and special characters are properly escaped
    """
    if not text or text == "N/A":
        return "N/A"
    
    # Remove excessive whitespace and replace newlines with spaces
    sanitized_text = re.sub(r'\s+', ' ', text).strip()
    
    # Escape double quotes and wrap in quotes if it contains commas
    if ',' in sanitized_text or '"' in sanitized_text:
        sanitized_text = '"' + sanitized_text.replace('"', '""') + '"'
    
    return sanitized_text

def load_firm_info():
    """Load firm contact info from JSON files."""
    firm_data = {}
    for file in os.listdir(PROCESSED_DIRECTORY):
        if file.endswith(".json") and file != "template_text.json":  # Ignore template_text.json
            with open(os.path.join(PROCESSED_DIRECTORY, file), "r", encoding="utf-8") as f:
                json_data = json.load(f)
                firm_info = json_data.get("firm_info", {})

                # If any of the firm fields are missing or "N/A", trigger the fallback method
                missing_data = any(
                    firm_info.get(field, "N/A") in ("", "N/A") for field in ["company", "address", "website", "name", "phone"]
                )

                extracted_contact_info = "N/A"
                if missing_data:
                    raw_text = json_data.get("text", "")
                    match = re.search(r"Contact Information(.*?)Form Generated on", raw_text, re.DOTALL)

                    if match:
                        extracted_contact_info = match.group(1).strip()
                        extracted_contact_info = sanitize_csv_text(extracted_contact_info)
                        print(f"[DEBUG] Extracted fallback text for {file}:")
                        print(extracted_contact_info[:500])  # Show first 500 characters for debugging
                    else:
                        print(f"[WARN] No firm info found in {file}, and no extractable text.")

                # If any field was missing, replace all firm info with extracted text
                if missing_data:
                    firm_data[file] = {
                        "company": extracted_contact_info,
                        "address": "N/A",
                        "website": "N/A",
                        "name": "N/A",
                        "phone": "N/A",
                    }
                else:
                    firm_data[file] = {
                        "company": firm_info.get("company", "N/A"),
                        "address": firm_info.get("address", "N/A"),
                        "website": firm_info.get("website", "N/A"),
                        "name": firm_info.get("name", "N/A"),
                        "phone": firm_info.get("phone", "N/A"),
                    }

                print(f"[INFO] Final firm info for {file}: {firm_data[file]}")
    
    return firm_data

def update_summary_with_contacts():
    """Add firm contact info to the summary report."""
    csv_file = find_latest_pdf_comparison()
    print(f"[INFO] Reading PDF comparison data from {csv_file}")
    
    with open(csv_file, "r", newline="", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        header = next(reader)
        rows = list(reader)

    overall_match_index = header.index("Overall_Match (%)")
    
    # Filter rows based on the Overall_Match threshold
    filtered_rows = [row for row in rows if float(row[overall_match_index]) >= 50.01]
    print(f"[INFO] Filtered rows count: {len(filtered_rows)}")

    if not filtered_rows:
        print("[WARN] No valid rows found after filtering. Exiting.")
        return

    firm_data = load_firm_info()
    
    new_header = header + ["company1", "address1", "website1", "name1", "phone1",
                            "company2", "address2", "website2", "name2", "phone2"]

    def get_firm_info(firm):
        """Ensure extracted text is used when structured data is missing."""
        company = firm["company"] if firm["company"] and firm["company"] != "N/A" else "N/A"
        return [company, firm["address"], firm["website"], firm["name"], firm["phone"]]

    new_rows = []
    for row in filtered_rows:
        file1, file2 = row[:2]
        
        firm1 = firm_data.get(file1, {"company": "N/A", "address": "N/A", "website": "N/A", "name": "N/A", "phone": "N/A"})
        firm2 = firm_data.get(file2, {"company": "N/A", "address": "N/A", "website": "N/A", "name": "N/A", "phone": "N/A"})
        
        new_row = row + get_firm_info(firm1) + get_firm_info(firm2)
        new_rows.append(new_row)

    print(f"[INFO] Writing updated summary report to {CSV_OUTPUT_FILE}")
    with open(CSV_OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(new_header)
        writer.writerows(new_rows)
    
    print(f"[INFO] Updated summary report saved to {CSV_OUTPUT_FILE}")

    # Debug: Print first few rows from the output CSV
    print("[DEBUG] First few rows of the output CSV:")
    with open(CSV_OUTPUT_FILE, "r", newline="", encoding="utf-8") as outfile:
        reader = csv.reader(outfile)
        output_header = next(reader)
        print(output_header)
        for i, row in enumerate(reader):
            print(row)
            if i >= 4:  # Limit to first 5 rows
                break

if __name__ == "__main__":
    update_summary_with_contacts()
