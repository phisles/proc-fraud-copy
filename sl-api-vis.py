import requests
from collections import defaultdict
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import streamlit as st
import pandas as pd
from streamlit_agraph import agraph, Node, Edge, Config

st.set_page_config(layout="wide", page_title="SBIR Awards Duplicate Finder")

BASE_URL = "https://api.www.sbir.gov/public/api/awards"

def fetch_page(start, agency, year, rows, page_number):
    params = {"agency": agency, "rows": rows, "start": start}
    if year:
        params["year"] = year
    st.sidebar.write(f"Requesting Page {page_number} | Start Offset: {start}")
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
    except requests.exceptions.RequestException as e:
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
    batch_size = 5
    progress_bar = st.sidebar.progress(0)
    status_text = st.sidebar.empty()

    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        while True:
            futures = {}
            for i in range(batch_size):
                current_start = start + i * rows
                current_page = page + i
                future = executor.submit(fetch_page, current_start, agency, year, rows, current_page)
                futures[future] = (current_start, current_page)
            
            batch_awards = []
            batch_empty = False
            for future in as_completed(futures):
                page_awards = future.result()
                curr_start, curr_page = futures[future]
                if not page_awards:
                    batch_empty = True
                else:
                    status_text.write(f"Page {curr_page}: Fetched {len(page_awards)} rows")
                    batch_awards.extend(page_awards)
            
            if not batch_awards and batch_empty:
                break
            
            results.extend(batch_awards)
            progress_value = min(len(results) / 10000, 1.0)
            progress_bar.progress(progress_value)

            start += batch_size * rows
            page += batch_size

    status_text.empty()
    progress_bar.empty()
    st.sidebar.write(f"\nTotal rows collected before filtering: {len(results)}")
    return results

def similar_address(addr1, addr2, threshold=0.8):
    if not addr1 or not addr2:
        return False
    addr1_clean = re.sub(r'(apt|suite|unit)\s*\.?\s*\d+', '', addr1, flags=re.IGNORECASE).strip()
    addr2_clean = re.sub(r'(apt|suite|unit)\s*\.?\s*\d+', '', addr2, flags=re.IGNORECASE).strip()

    num1 = re.search(r'\d+', addr1_clean)
    num2 = re.search(r'\d+', addr2_clean)
    if num1 and num2 and num1.group() != num2.group():
        return False
    return SequenceMatcher(None, addr1_clean.lower(), addr2_clean.lower()).ratio() > threshold

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

    def add_edge_if_firms_differ(idx1, idx2):
        firm_i = normalize_firm_name(awards[idx1]["firm"])
        firm_j = normalize_firm_name(awards[idx2]["firm"])
        if firm_i != firm_j:
            graph[idx1].add(idx2)
            graph[idx2].add(idx1)

    url_to_indices = defaultdict(list)
    for i, award in enumerate(awards):
        url = (award.get("company_url") or "").strip()
        if url and url.lower() != "none":
            url_to_indices[url].append(i)
    for indices in url_to_indices.values():
        for i in indices:
            for j in indices:
                if i != j:
                    add_edge_if_firms_differ(i, j)

    phone_to_indices = defaultdict(list)
    for i, award in enumerate(awards):
        for field in ["poc_phone", "pi_phone"]:
            phone = (award.get(field) or "").strip()
            if phone and phone.lower() != "none":
                phone_to_indices[phone].append(i)
    for indices in phone_to_indices.values():
        for i in indices:
            for j in indices:
                if i != j:
                    add_edge_if_firms_differ(i, j)

    for i in range(n):
        addr_i = (awards[i].get("address1") or "").strip()
        if not addr_i or addr_i.lower() == "none":
            continue
        for j in range(i+1, n):
            addr_j = (awards[j].get("address1") or "").strip()
            if not addr_j or addr_j.lower() == "none":
                continue
            if similar_address(addr_i, addr_j):
                add_edge_if_firms_differ(i, j)

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


# --- MODIFIED display_graph to take a single component ---
def display_graph_for_component(awards, component_indices):
    nodes = []
    edges = []
    
    node_ids = set() # To ensure unique IDs within this single component's graph

    NODE_COLOR_FIRM = "#4285F4" 
    NODE_COLOR_URL = "#EA4335"  
    NODE_COLOR_ADDRESS = "#34A853"
    NODE_COLOR_PHONE = "#FBBC04"

    firm_nodes_map = {} # Map firm_name to a single node ID for that firm within this group

    for award_idx in component_indices:
        award = awards[award_idx]
        firm_name = normalize_firm_name(award.get("firm", "Unknown Firm"))
        
        # Create a firm node. If multiple awards in this component are from the same firm,
        # they should point to the same firm node to make the graph cleaner for that group.
        # Use an ID that is unique to the firm within this group, not across all awards.
        firm_node_id_for_group = f"firm_node_{firm_name}"
        if firm_node_id_for_group not in node_ids:
            nodes.append(Node(id=firm_node_id_for_group, label=firm_name, size=30, color=NODE_COLOR_FIRM, shape="dot", font={"size": 14}))
            node_ids.add(firm_node_id_for_group)
            firm_nodes_map[firm_name] = firm_node_id_for_group # Store unique firm node for this group

        current_firm_node_id = firm_nodes_map[firm_name]

        # Add URL node and edge
        company_url = (award.get("company_url") or "").strip()
        if company_url and company_url.lower() != "none":
            url_id = f"url_node_{company_url}"
            if url_id not in node_ids:
                nodes.append(Node(id=url_id, label=company_url, size=20, color=NODE_COLOR_URL, shape="box", font={"size": 12}))
                node_ids.add(url_id)
            # Add edge from this firm instance to the URL node
            edges.append(Edge(source=current_firm_node_id, target=url_id, label="HAS_URL", type="arrow", color={"color": "#cccccc"}))

        # Add Address node and edge
        address = (award.get("address1") or "").strip()
        if address and address.lower() != "none":
            address_id = f"address_node_{address}"
            if address_id not in node_ids:
                nodes.append(Node(id=address_id, label=address, size=25, color=NODE_COLOR_ADDRESS, shape="hexagon", font={"size": 12}))
                node_ids.add(address_id)
            # Add edge from this firm instance to the Address node
            edges.append(Edge(source=current_firm_node_id, target=address_id, label="LOCATED_AT", type="arrow", color={"color": "#cccccc"}))

        # Add Phone nodes and edges
        for field in ["poc_phone", "pi_phone"]:
            phone = (award.get(field) or "").strip()
            if phone and phone.lower() != "none":
                phone_id = f"phone_node_{phone}"
                if phone_id not in node_ids:
                    nodes.append(Node(id=phone_id, label=phone, size=20, color=NODE_COLOR_PHONE, shape="triangle", font={"size": 12}))
                    node_ids.add(phone_id)
                # Add edge from this firm instance to the Phone node
                edges.append(Edge(source=current_firm_node_id, target=phone_id, label="HAS_PHONE", type="arrow", color={"color": "#cccccc"}))
    
    if not nodes:
        st.info("No nodes to display in this graph group.")
        return

    config = Config(
        width=800, # Smaller width for individual graphs
        height=500, # Smaller height for individual graphs
        directed=True,
        nodeHighlightBehavior=True,
        highlightColor="#F7A7A6",
        collapsible=True,
        node={"labelProperty": "label", "font": {"size": 12}},
        link={"labelProperty": "label", "renderLabel": True, "font": {"size": 10}},
        physics={"enabled": True, "solver": "barnesHut", "barnesHut": {"gravitationalConstant": -1000, "centralGravity": 0.1, "springLength": 80, "springConstant": 0.05, "damping": 0.09, "avoidOverlap": 0.5}},
    )

    agraph(nodes=nodes, edges=edges, config=config)


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

    col1, col2, col3 = st.columns(3)
    col1.metric(label="Total Awards Analyzed", value=total_awards)
    col2.metric(label="Duplicate Entities", value=duplicate_entities)
    col3.metric(label="Total Award Amount for Duplicates", value=f"${total_duplicates_amount:,.2f}")

    st.markdown("---") 
    st.header("Interactive Duplicate Graphs by Group") # New header for individual graphs

    # Iterate through each component and display its graph and table
    for comp_index, comp in enumerate(components):
        comp_rows = [awards[i] for i in comp]
        distinct_firms = sorted(set(normalize_firm_name(row["firm"]) for row in comp_rows))
        
        # Use st.expander to make each group collapsible, which improves UI
        with st.expander(f"Group {comp_index + 1}: Firms - {', '.join(distinct_firms)}", expanded=False):
            st.markdown(f"#### Graph for Group {comp_index + 1}")
            display_graph_for_component(awards, comp) # Pass only the current component
            st.markdown("---")
            
            st.markdown(f"#### Details for Group {comp_index + 1}")
            df = pd.DataFrame(sorted(comp_rows, key=lambda a: normalize_firm_name(a.get("firm", ""))))
            
            required_cols = ["firm", "company_url", "address1", "address2", "poc_phone", "pi_phone", "ri_poc_phone", "award_link", "agency", "branch", "award_amount"]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = "N/A"

            df["Link"] = df["award_link"].apply(
                lambda x: f'<a href="https://www.sbir.gov/awards/{x}" target="_blank">link</a>' if x and x != "N/A" else "N/A"
            )
            
            display_cols = ["firm", "company_url", "address1", "address2", "poc_phone", "pi_phone", "ri_poc_phone", "Link", "agency", "branch", "award_amount"]
            df_display = df[[col for col in display_cols if col in df.columns]]

            st.markdown(df_display.to_html(escape=False), unsafe_allow_html=True)
            
            group_total = 0.0
            for award in comp_rows:
                try:
                    group_total += float(award.get("award_amount", 0))
                except ValueError:
                    group_total += 0
            st.write(f"**Total Award Amount for this group:** ${group_total:,.2f}")
            
        st.markdown("---") # Add a horizontal rule between groups for readability

def main():
    st.title("AF OSI Procurement Fraud Tool V1")
    
    st.sidebar.header("Filters")
    year = st.sidebar.number_input("Year", value=2023, step=1, help="Year to fetch SBIR awards from.")
    agency = st.sidebar.text_input("Agency", "DOD", help="e.g., DOD, DOE, NIH. Case-insensitive.")
    branch = st.sidebar.text_input("Branch (optional)", "USAF", help="e.g., USAF, Army, Navy. Leave blank for all branches within the agency.")
    
    run_clicked = st.sidebar.button("Run Analysis")
    
    if not run_clicked:
        st.info("Adjust the filters in the sidebar and click 'Run Analysis' to fetch data.")
    else:
        st.sidebar.write("---")
        st.sidebar.write("Starting data fetch...")
        
        with st.spinner('Fetching awards data... This might take a while for large datasets.'):
            awards = fetch_awards(agency=agency, year=year, rows=100)
        
        if not awards:
            st.warning("No awards data fetched. Please check the filters and try again.")
            return

        if branch.strip():
            filtered_awards = [award for award in awards if award.get("branch", "").upper() == branch.upper()]
            st.sidebar.write(f"Total rows after branch filtering ({branch.upper()}): {len(filtered_awards)}")
        else:
            filtered_awards = awards
            st.sidebar.write(f"Total rows fetched: {len(filtered_awards)}")
        
        if not filtered_awards:
            st.warning("No awards found after applying branch filter. Try a different branch or leave it blank.")
            return

        st.sidebar.write("Running duplicate analysis...")
        with st.spinner('Analyzing duplicates and building graph...'):
            display_results(filtered_awards)
        
        st.success("Analysis complete!")

if __name__ == "__main__":
    main()
