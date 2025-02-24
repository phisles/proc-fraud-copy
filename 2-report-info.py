import os
import json
import csv

PROCESSED_DIRECTORY = "./processed_data"
SUMMARY_REPORT_FILE = "summary_report.csv"
UPDATED_SUMMARY_REPORT_FILE = "summary_report_with_contacts.csv"

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
    print("Loading firm contact info...")
    firm_data = load_firm_info()
    
    print(f"Reading existing summary report from {SUMMARY_REPORT_FILE}")
    with open(SUMMARY_REPORT_FILE, "r", newline="", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        header = next(reader)
        rows = list(reader)
    
    new_header = header + ["company1", "address1", "website1", "name1", "phone1", 
                            "company2", "address2", "website2", "name2", "phone2"]
    
    new_rows = []
    for row in rows:
        file = row[0]  # First column is the primary file
        match = row[1] if row[1] else ""  # Second column contains the match file
        
        print(f"Processing file: {file} with match: {match}")
        
        firm1 = firm_data.get(file, {"company": "", "address": "", "website": "", "name": "", "phone": ""})
        firm2 = firm_data.get(match, {"company": "", "address": "", "website": "", "name": "", "phone": ""})
        
        new_row = row + [firm1["company"], firm1["address"], firm1["website"], firm1["name"], firm1["phone"],
                          firm2["company"], firm2["address"], firm2["website"], firm2["name"], firm2["phone"]]
        print(f"Updated row: {new_row}")
        new_rows.append(new_row)
    
    print(f"Writing updated summary report to {UPDATED_SUMMARY_REPORT_FILE}")
    with open(UPDATED_SUMMARY_REPORT_FILE, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(new_header)
        writer.writerows(new_rows)
    
    print(f"Updated summary report saved to {UPDATED_SUMMARY_REPORT_FILE}")

if __name__ == "__main__":
    update_summary_with_contacts()
