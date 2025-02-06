import requests
from prettytable import PrettyTable
from collections import defaultdict
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

BASE_URL = "https://api.www.sbir.gov/public/api/awards"

def fetch_page(start, agency, year, rows, page_number):
    params = {"agency": agency, "rows": rows, "start": start}
    if year:
        params["year"] = year
    print(f"Requesting Page {page_number} | Start Offset: {start}")
    try:
        response = requests.get(BASE_URL, params=params)
    except Exception as e:
        print(f"Error fetching page {page_number}: {e}")
        return []
    if response.status_code != 200:
        print(f"Error: Unable to fetch data (Status Code: {response.status_code})")
        return []
    data = response.json()
    if isinstance(data, list):
        return data
    print("Unexpected response format, stopping pagination.")
    return []

def fetch_awards(agency="DOD", year=None, rows=100):
    """
    Fetches all SBIR awards using multithreading for pagination.
    - agency: Agency to search (default: DOD).
    - year: Specific year to filter (default: None).
    - rows: Number of records per request (default: 100).
    """
    results = []
    start = 0
    page = 1
    batch_size = 10  # Number of pages to fetch concurrently

    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        while True:
            futures = {}
            for i in range(batch_size):
                current_start = start + i * rows
                current_page = page + i
                future = executor.submit(fetch_page, current_start, agency, year, rows, current_page)
                futures[future] = (current_start, current_page)
            batch_empty = False
            for future in as_completed(futures):
                page_awards = future.result()
                curr_start, curr_page = futures[future]
                if not page_awards:
                    batch_empty = True
                else:
                    print(f"Page {curr_page}: Fetched {len(page_awards)} rows")
                    results.extend(page_awards)
            if batch_empty:
                break
            start += batch_size * rows
            page += batch_size

    print(f"\nTotal rows collected before filtering: {len(results)}")
    return results

def similar_address(addr1, addr2, threshold=0.8):
    """Check if two addresses are similar using sequence matching with numeric comparison."""
    if not addr1 or not addr2:
        return False
    num1 = re.search(r'\d+', addr1)
    num2 = re.search(r'\d+', addr2)
    if num1 and num2 and num1.group() != num2.group():
        return False
    return SequenceMatcher(None, addr1.lower(), addr2.lower()).ratio() > threshold

def normalize_firm_name(name):
    """Normalize firm names by lowercasing, removing punctuation, extra whitespace, and common suffixes."""
    n = re.sub(r'\s+', ' ', name).strip()
    n = n.lower().replace(".", "").replace(",", "")
    for suffix in [" inc", " llc", " corporation", " corp", " co"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    return n

def find_duplicate_components(awards):
    """
    Build a graph of rows (nodes) where an edge exists if two rows share a matching value
    (company URL, phone, or similar address) and have different normalized firm names.
    Returns a list of connected components (each a list of indices) that contain at least two rows
    and at least two distinct normalized firm names.
    """
    # Filter out any None entries
    awards = [award for award in awards if award is not None]
    n = len(awards)
    graph = {i: set() for i in range(n)}

    # Group by URL.
    url_to_indices = defaultdict(list)
    for i, award in enumerate(awards):
        url = (award.get("company_url") or "").strip()
        if url and url.lower() != "none":
            url_to_indices[url].append(i)
    for key, indices in url_to_indices.items():
        firms = [normalize_firm_name(awards[i]["firm"]) for i in indices]
        if len(set(firms)) > 1:
            for i in indices:
                for j in indices:
                    if i != j:
                        graph[i].add(j)
                        graph[j].add(i)

    # Group by phone (using both poc_phone and pi_phone).
    phone_to_indices = defaultdict(list)
    for i, award in enumerate(awards):
        for field in ["poc_phone", "pi_phone"]:
            phone = (award.get(field) or "").strip()
            if phone and phone.lower() != "none":
                phone_to_indices[phone].append(i)
    for key, indices in phone_to_indices.items():
        firms = [normalize_firm_name(awards[i]["firm"]) for i in indices]
        if len(set(firms)) > 1:
            for i in indices:
                for j in indices:
                    if i != j:
                        graph[i].add(j)
                        graph[j].add(i)

    # Pairwise check for similar addresses.
    for i in range(n):
        addr_i = (awards[i].get("address1") or "").strip()
        if not addr_i or addr_i.lower() == "none":
            continue
        for j in range(i+1, n):
            addr_j = (awards[j].get("address1") or "").strip()
            if not addr_j or addr_j.lower() == "none":
                continue
            if similar_address(addr_i, addr_j):
                firm_i = normalize_firm_name(awards[i]["firm"])
                firm_j = normalize_firm_name(awards[j]["firm"])
                if firm_i != firm_j:
                    graph[i].add(j)
                    graph[j].add(i)

    # Find connected components.
    seen = set()
    components = []
    for i in range(n):
        if i not in seen:
            stack = [i]
            comp = []
            while stack:
                cur = stack.pop()
                if cur in seen:
                    continue
                seen.add(cur)
                comp.append(cur)
                for neigh in graph[cur]:
                    if neigh not in seen:
                        stack.append(neigh)
            components.append(comp)

    # Only keep components with at least two rows and with differing normalized firm names.
    final_components = []
    for comp in components:
        if len(comp) < 2:
            continue
        firm_set = set(normalize_firm_name(awards[i]["firm"]) for i in comp)
        if len(firm_set) > 1:
            final_components.append(comp)
    return final_components

def display_results(awards):
    """
    Displays one table per duplicate component.
    Each table is titled with the distinct firm names involved,
    and shows all rows (with award link) for the investigation.
    """
    components = find_duplicate_components(awards)
    total_duplicates_amount = 0.0
    if not components:
        print("No matching groups found where rows with different firm names share a common value.")
        return
    for comp in components:
        # Gather the rows for this component.
        comp_rows = [awards[i] for i in comp]
        distinct_firms = sorted(set(normalize_firm_name(row["firm"]) for row in comp_rows))
        title = f"Duplicate Firms: {', '.join(distinct_firms)}"
        print(f"\n{title}")
        table = PrettyTable()
        table.field_names = [
            "firm", "company_url", "address1", "address2",
            "poc_phone", "pi_phone", "ri_poc_phone",
            "award_link", "agency", "branch", "award_amount"
        ]
        for award in sorted(comp_rows, key=lambda a: normalize_firm_name(a.get("firm", ""))):
            table.add_row([
                award.get("firm", "N/A"),
                award.get("company_url", "N/A"),
                award.get("address1", "N/A"),
                award.get("address2", "N/A"),
                award.get("poc_phone", "N/A"),
                award.get("pi_phone", "N/A"),
                award.get("ri_poc_phone", "N/A"),
                f"https://legacy.www.sbir.gov/node/{award.get('award_link', 'N/A')}",
                award.get("agency", "N/A"),
                award.get("branch", "N/A"),
                award.get("award_amount", "N/A")
            ])
        print(table)
        group_total = 0.0
        for award in comp_rows:
            try:
                group_total += float(award.get("award_amount", 0))
            except ValueError:
                group_total += 0
        total_duplicates_amount += group_total
        print(f"Total Award Amount for these duplicates: {group_total}")
    duplicate_entities = sum(len(comp) for comp in components)
    print(f"\nSummary: Total awards analyzed: {len(awards)}")
    print(f"Summary: Total duplicate entities: {duplicate_entities}")
    print(f"Summary: Total Award Amount for duplicate entities with different firm names: {total_duplicates_amount}")

if __name__ == "__main__":
    agency = "DOD"
    year = 2023
    rows = 100
    awards = fetch_awards(agency=agency, year=year, rows=rows)
    
    # Filter the results by branch: only rows with branch 'USAF'
    filtered_awards = [award for award in awards if award.get("branch", "").upper() == "USAF"]
    print(f"\nTotal rows after branch filtering (USAF): {len(filtered_awards)}\n")
    
    display_results(filtered_awards)