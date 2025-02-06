import requests
from collections import defaultdict
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import streamlit as st
import pandas as pd

st.set_page_config(layout="wide", page_title="SBIR Awards Duplicate Finder")

BASE_URL = "https://api.www.sbir.gov/public/api/awards"

def fetch_page(start, agency, year, rows, page_number):
    params = {"agency": agency, "rows": rows, "start": start}
    if year:
        params["year"] = year
    st.sidebar.write(f"Requesting Page {page_number} | Start Offset: {start}")
    try:
        response = requests.get(BASE_URL, params=params)
    except Exception as e:
        st.sidebar.write(f"Error fetching page {page_number}: {e}")
        return []
    if response.status_code != 200:
        st.sidebar.write(f"Error: Unable to fetch data (Status Code: {response.status_code})")
        return []
    data = response.json()
    if isinstance(data, list):
        return data
    st.sidebar.write("Unexpected response format, stopping pagination.")
    return []

def fetch_awards(agency="DOD", year=None, rows=100):
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
                    st.sidebar.write(f"Page {curr_page}: Fetched {len(page_awards)} rows")
                    results.extend(page_awards)
            if batch_empty:
                break
            start += batch_size * rows
            page += batch_size
    st.sidebar.write(f"\nTotal rows collected before filtering: {len(results)}")
    return results

def similar_address(addr1, addr2, threshold=0.8):
    if not addr1 or not addr2:
        return False
    num1 = re.search(r'\d+', addr1)
    num2 = re.search(r'\d+', addr2)
    if num1 and num2 and num1.group() != num2.group():
        return False
    return SequenceMatcher(None, addr1.lower(), addr2.lower()).ratio() > threshold

def normalize_firm_name(name):
    n = re.sub(r'\s+', ' ', name).strip()
    n = n.lower().replace(".", "").replace(",", "")
    for suffix in [" inc", " llc", " corporation", " corp", " co"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    return n

def find_duplicate_components(awards):
    awards = [award for award in awards if award is not None]
    n = len(awards)
    graph = {i: set() for i in range(n)}
    # Group by URL.
    url_to_indices = defaultdict(list)
    for i, award in enumerate(awards):
        url = (award.get("company_url") or "").strip()
        if url and url.lower() != "none":
            url_to_indices[url].append(i)
    for indices in url_to_indices.values():
        firms = [normalize_firm_name(awards[i]["firm"]) for i in indices]
        if len(set(firms)) > 1:
            for i in indices:
                for j in indices:
                    if i != j:
                        graph[i].add(j)
                        graph[j].add(i)
    # Group by phone.
    phone_to_indices = defaultdict(list)
    for i, award in enumerate(awards):
        for field in ["poc_phone", "pi_phone"]:
            phone = (award.get(field) or "").strip()
            if phone and phone.lower() != "none":
                phone_to_indices[phone].append(i)
    for indices in phone_to_indices.values():
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
    final_components = []
    for comp in components:
        if len(comp) < 2:
            continue
        firm_set = set(normalize_firm_name(awards[i]["firm"]) for i in comp)
        if len(firm_set) > 1:
            final_components.append(comp)
    return final_components

def display_results(awards):
    components = find_duplicate_components(awards)
    if not components:
        st.write("No matching groups found where rows with different firm names share a common value.")
        return

    # Compute summary info
    total_duplicates_amount = 0.0
    for comp in components:
        comp_rows = [awards[i] for i in comp]
        group_total = 0.0
        for award in comp_rows:
            try:
                group_total += float(award.get("award_amount", 0))
            except ValueError:
                group_total += 0
        total_duplicates_amount += group_total
    duplicate_entities = sum(len(comp) for comp in components)
    total_awards = len(awards)

    # Display summary at the top in a colored, large section.
    col1, col2, col3 = st.columns(3)
    col1.metric(label="Total Awards Analyzed", value=total_awards)
    col2.metric(label="Duplicate Entities", value=duplicate_entities)
    col3.metric(label="Total Award Amount", value=f"${total_duplicates_amount:,.2f}")

    # Now display each duplicate group.
    for comp in components:
        comp_rows = [awards[i] for i in comp]
        distinct_firms = sorted(set(normalize_firm_name(row["firm"]) for row in comp_rows))
        title = f"Duplicate Firms: {', '.join(distinct_firms)}"
        st.subheader(title)
        df = pd.DataFrame(sorted(comp_rows, key=lambda a: normalize_firm_name(a.get("firm", ""))))
        df["Link"] = df["award_link"].apply(
            lambda x: f'<a href="https://www.sbir.gov/awards/{x}" target="_blank">link</a>' if x != "N/A" else "N/A"
        )
        df = df[["firm", "company_url", "address1", "address2", "poc_phone", "pi_phone", "ri_poc_phone", "Link", "agency", "branch", "award_amount"]]
        st.markdown(df.to_html(escape=False), unsafe_allow_html=True)
        group_total = 0.0
        for award in comp_rows:
            try:
                group_total += float(award.get("award_amount", 0))
            except ValueError:
                group_total += 0
        st.write(f"Total Award Amount for these duplicates: {group_total}")

def main():
    st.title("AF OSI Procurement Fraud Tool V1")
    year = st.sidebar.number_input("Year", value=2023, step=1)
    agency = st.sidebar.text_input("Agency", "DOD")
    branch = st.sidebar.text_input("Branch (optional, leave blank for all)", "USAF")
    run_clicked = st.sidebar.button("Run")
    if not run_clicked:
        st.info("Adjust the filters in the sidebar and click 'Run' to fetch data.")
    else:
        st.sidebar.write("Fetching awards data...")
        awards = fetch_awards(agency=agency, year=year, rows=100)
        if branch.strip():
            filtered_awards = [award for award in awards if award.get("branch", "").upper() == branch.upper()]
            st.sidebar.write(f"Total rows after branch filtering ({branch.upper()}): {len(filtered_awards)}")
        else:
            filtered_awards = awards
            st.sidebar.write(f"Total rows fetched: {len(filtered_awards)}")
        display_results(filtered_awards)

if __name__ == "__main__":
    main()