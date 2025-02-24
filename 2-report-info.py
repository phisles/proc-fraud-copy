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
    print(f"Using latest PDF comparison file: {latest_file}")
    return os.path.join(CSV_DIRECTORY, latest_file)

def load_firm_info():
    """Load firm contact info from JSON files."""
    firm_data = {}
    for file in os.listdir(PROCESSED_DIRECTORY):
        if file.endswith(".json") and file != "template_text.json":  # Ignore template_text.json
            with open(os.path.join(PROCESSED_DIRECTORY, file), "r", encoding="utf-8") as f:
                json_data = json.load(f)
                firm_info = json_data.get("firm_info", {})
                firm_data[file] = {
                    "company": firm_info.get("company", ""),
                    "address": firm_info.get("address", ""),
                    "website": firm_info.get("website", ""),
                    "name": firm_info.get("name", ""),
                    "phone": firm_info.get("phone", "")
                }
                print(f"Loaded firm info for {file}: {firm_data[file]}")
    return firm_data

def update_summary_with_contacts():
    """Add firm contact info to the summary report."""
    csv_file = find_latest_pdf_comparison()
    print(f"Reading PDF comparison data from {csv_file}")
    
    with open(csv_file, "r", newline="", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        header = next(reader)
        rows = list(reader)

    overall_match_index = header.index("Overall_Match (%)")
    
    # Filter rows based on the Overall_Match threshold
    filtered_rows = [row for row in rows if float(row[overall_match_index]) >= 50.01]
    print(f"Filtered rows count: {len(filtered_rows)}")

    if not filtered_rows:
        print("No valid rows found after filtering. Exiting.")
        return

    firm_data = load_firm_info()
    
    new_header = header + ["company1", "address1", "website1", "name1", "phone1",
                            "company2", "address2", "website2", "name2", "phone2"]
    
    new_rows = []
    for row in filtered_rows:
        file1 = row[0]  # First column is the primary file
        file2 = row[1]  # Second column contains the match file
        
        print(f"Processing file: {file1} with match: {file2}")
        
        firm1 = firm_data.get(file1, {"company": "", "address": "", "website": "", "name": "", "phone": ""})
        firm2 = firm_data.get(file2, {"company": "", "address": "", "website": "", "name": "", "phone": ""})
        
        new_row = row + [firm1["company"], firm1["address"], firm1["website"], firm1["name"], firm1["phone"],
                          firm2["company"], firm2["address"], firm2["website"], firm2["name"], firm2["phone"]]
        print(f"Updated row: {new_row}")
        new_rows.append(new_row)
    
    print(f"Writing updated summary report to {CSV_OUTPUT_FILE}")
    with open(CSV_OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(new_header)
        writer.writerows(new_rows)
    
    print(f"Updated summary report saved to {CSV_OUTPUT_FILE}")

if __name__ == "__main__":
    update_summary_with_contacts()
