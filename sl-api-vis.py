import requests
from collections import defaultdict
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import streamlit as st
import pandas as pd
from streamlit_agraph import agraph, Node, Edge, Config # Import for graph visualization

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
    batch_size = 10
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


def display_graph_for_component(awards, component_indices):
    nodes = []
    edges = []
    
    node_ids = set() # To ensure unique IDs within this single component's graph

    # Define colors for different node types
    NODE_COLOR_FIRM = "#4285F4" # Blue
    NODE_COLOR_URL = "#34A853"  # Green
    NODE_COLOR_ADDRESS = "#FBBC04" # Yellow
    NODE_COLOR_PHONE = "#EA4335" # Red

    # Highlight colors for shared (red flag) attributes
    HIGHLIGHT_COLOR_NODE = "#FF0000" # Bright Red for the shared node
    HIGHLIGHT_COLOR_EDGE = "#FF0000" # Bright Red for the connecting edges
    HIGHLIGHT_EDGE_WIDTH = 3
    HIGHLIGHT_NODE_BORDER_COLOR = "#FFFFFF" # White border for highlight
    HIGHLIGHT_NODE_BORDER_WIDTH = 3
    HIGHLIGHT_NODE_SIZE_INCREASE = 1.5 # Make it 50% larger

    firm_nodes_map = {} # Map firm_name to a single node ID for that firm within this group

    # --- Step 1: Identify shared "red flag" attributes within this component ---
    shared_urls = defaultdict(set)
    shared_addresses = defaultdict(set)
    shared_phones = defaultdict(set)

    for award_idx in component_indices:
        award = awards[award_idx]
        firm_name = normalize_firm_name(award.get("firm", "Unknown Firm"))
        
        company_url = (award.get("company_url") or "").strip()
        if company_url and company_url.lower() != "none":
            shared_urls[company_url].add(firm_name)

        address = (award.get("address1") or "").strip()
        if address and address.lower() != "none":
            shared_addresses[address].add(firm_name)

        for field in ["poc_phone", "pi_phone"]:
            phone = (award.get(field) or "").strip()
            if phone and phone.lower() != "none":
                shared_phones[phone].add(firm_name)

    # Filter to only include attributes shared by MORE THAN ONE firm
    red_flag_urls = {url for url, firms in shared_urls.items() if len(firms) > 1}
    red_flag_addresses = {addr for addr, firms in shared_addresses.items() if len(firms) > 1}
    red_flag_phones = {phone for phone, firms in shared_phones.items() if len(firms) > 1}
    # --- End Step 1 ---


    for award_idx in component_indices:
        award = awards[award_idx]
        firm_name = normalize_firm_name(award.get("firm", "Unknown Firm"))
        
        firm_node_id_for_group = f"firm_node_{firm_name}"
        if firm_node_id_for_group not in node_ids:
            nodes.append(Node(id=firm_node_id_for_group, label=firm_name, size=30, color=NODE_COLOR_FIRM, shape="dot", font={"size": 14}))
            node_ids.add(firm_node_id_for_group)
            firm_nodes_map[firm_name] = firm_node_id_for_group

        current_firm_node_id = firm_nodes_map[firm_name]

        # Add URL node and edge
        company_url = (award.get("company_url") or "").strip()
        if company_url and company_url.lower() != "none":
            url_id = f"url_node_{company_url}"
            is_red_flag_url = url_id in {f"url_node_{u}" for u in red_flag_urls} # Check if this specific URL is a red flag

            # Node styling
            url_node_color = HIGHLIGHT_COLOR_NODE if is_red_flag_url else NODE_COLOR_URL
            url_node_size = 20 * HIGHLIGHT_NODE_SIZE_INCREASE if is_red_flag_url else 20
            url_node_border = {"color": HIGHLIGHT_NODE_BORDER_COLOR, "width": HIGHLIGHT_NODE_BORDER_WIDTH} if is_red_flag_url else None
            url_node_shape = "star" if is_red_flag_url else "box" # Change shape for emphasis

            if url_id not in node_ids:
                nodes.append(Node(id=url_id, label=company_url, size=url_node_size, color=url_node_color, shape=url_node_shape, font={"size": 12}, borderWidth=url_node_border.get("width", 1) if url_node_border else 1, borderColor=url_node_border.get("color", "black") if url_node_border else "black"))
                node_ids.add(url_id)
            
            # Edge styling
            edge_color = {"color": HIGHLIGHT_COLOR_EDGE} if is_red_flag_url else {"color": "#cccccc"}
            edge_width = HIGHLIGHT_EDGE_WIDTH if is_red_flag_url else 1

            edges.append(Edge(source=current_firm_node_id, target=url_id, label="HAS_URL", type="arrow", color=edge_color, width=edge_width))

        # Add Address node and edge
        address = (award.get("address1") or "").strip()
        if address and address.lower() != "none":
            address_id = f"address_node_{address}"
            is_red_flag_address = address_id in {f"address_node_{a}" for a in red_flag_addresses}

            address_node_color = HIGHLIGHT_COLOR_NODE if is_red_flag_address else NODE_COLOR_ADDRESS
            address_node_size = 25 * HIGHLIGHT_NODE_SIZE_INCREASE if is_red_flag_address else 25
            address_node_border = {"color": HIGHLIGHT_NODE_BORDER_COLOR, "width": HIGHLIGHT_NODE_BORDER_WIDTH} if is_red_flag_address else None
            address_node_shape = "star" if is_red_flag_address else "hexagon"

            if address_id not in node_ids:
                nodes.append(Node(id=address_id, label=address, size=address_node_size, color=address_node_color, shape=address_node_shape, font={"size": 12}, borderWidth=address_node_border.get("width", 1) if address_node_border else 1, borderColor=address_node_border.get("color", "black") if address_node_border else "black"))
                node_ids.add(address_id)
            
            edge_color = {"color": HIGHLIGHT_COLOR_EDGE} if is_red_flag_address else {"color": "#cccccc"}
            edge_width = HIGHLIGHT_EDGE_WIDTH if is_red_flag_address else 1

            edges.append(Edge(source=current_firm_node_id, target=address_id, label="LOCATED_AT", type="arrow", color=edge_color, width=edge_width))

        # Add Phone nodes and edges
        for field in ["poc_phone", "pi_phone"]:
            phone = (award.get(field) or "").strip()
            if phone and phone.lower() != "none":
                phone_id = f"phone_node_{phone}"
                is_red_flag_phone = phone_id in {f"phone_node_{p}" for p in red_flag_phones}

                phone_node_color = HIGHLIGHT_COLOR_NODE if is_red_flag_phone else NODE_COLOR_PHONE
                phone_node_size = 20 * HIGHLIGHT_NODE_SIZE_INCREASE if is_red_flag_phone else 20
                phone_node_border = {"color": HIGHLIGHT_NODE_BORDER_COLOR, "width": HIGHLIGHT_NODE_BORDER_WIDTH} if is_red_flag_phone else None
                phone_node_shape = "star" if is_red_flag_phone else "triangle"

                if phone_id not in node_ids:
                    nodes.append(Node(id=phone_id, label=phone, size=phone_node_size, color=phone_node_color, shape=phone_node_shape, font={"size": 12}, borderWidth=phone_node_border.get("width", 1) if phone_node_border else 1, borderColor=phone_node_border.get("color", "black") if phone_node_border else "black"))
                    node_ids.add(phone_id)
                
                edge_color = {"color": HIGHLIGHT_COLOR_EDGE} if is_red_flag_phone else {"color": "#cccccc"}
                edge_width = HIGHLIGHT_EDGE_WIDTH if is_red_flag_phone else 1

                edges.append(Edge(source=current_firm_node_id, target=phone_id, label="HAS_PHONE", type="arrow", color=edge_color, width=edge_width))
    
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
    st.header("Interactive Duplicate Graphs by Group")

    for comp_index, comp in enumerate(components):
        comp_rows = [awards[i] for i in comp]
        distinct_firms = sorted(set(normalize_firm_name(row["firm"]) for row in comp_rows))
        
        with st.expander(f"Group {comp_index + 1}: Firms - {', '.join(distinct_firms)}", expanded=False):
            st.markdown(f"#### Graph for Group {comp_index + 1}")
            display_graph_for_component(awards, comp)
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
            
        st.markdown("---")

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
