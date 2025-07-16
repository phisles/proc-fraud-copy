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

# --- MODIFIED fetch_page FUNCTION ---
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
        # Extract individual address components
        processed_data = []
        for award in data:
            # Create new keys for address2, city, state, zip
            award['address2'] = award.get('address2', '').strip()
            award['city'] = award.get('city', '').strip()
            award['state'] = award.get('state', '').strip()
            award['zip'] = award.get('zip', '').strip()
            processed_data.append(award)
        return processed_data
    st.sidebar.write("Unexpected response format, stopping pagination.")
    return []

# --- ORIGINAL WORKING fetch_awards FUNCTION ---
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

# --- find_duplicate_components (as it was in the last version where detection worked) ---
def find_duplicate_components(awards):
    awards = [award for award in awards if award is not None]
    n = len(awards)
    graph = {i: set() for i in range(n)}
    edge_reasons = defaultdict(set)

    def add_edge_if_firms_differ(idx1, idx2, reason):
        firm_i = normalize_firm_name(awards[idx1]["firm"])
        firm_j = normalize_firm_name(awards[idx2]["firm"])
        if firm_i != firm_j:
            graph[idx1].add(idx2)
            graph[idx2].add(idx1)
            edge_reasons[tuple(sorted((idx1, idx2)))].add(reason)

    url_to_indices = defaultdict(list)
    for i, award in enumerate(awards):
        url = (award.get("company_url") or "").strip()
        if url and url.lower() != "none":
            url_to_indices[url].append(i)
    for url, indices in url_to_indices.items():
        for i in indices:
            for j in indices:
                if i != j:
                    add_edge_if_firms_differ(i, j, f"shared_url:{url}")

    phone_to_indices = defaultdict(list)
    for i, award in enumerate(awards):
        for field in ["poc_phone", "pi_phone"]:
            phone = (award.get(field) or "").strip()
            if phone and phone.lower() != "none":
                phone_to_indices[phone].append(i)
    for phone, indices in phone_to_indices.items():
        for i in indices:
            for j in indices:
                if i != j:
                    add_edge_if_firms_differ(i, j, f"shared_phone:{phone}")

    for i in range(n):
        addr_i = (awards[i].get("address1") or "").strip()
        if not addr_i or addr_i.lower() == "none":
            continue
        for j in range(i+1, n):
            addr_j = (awards[j].get("address1") or "").strip()
            if not addr_j or addr_j.lower() == "none":
                continue
            if similar_address(addr_i, addr_j):
                add_edge_if_firms_differ(i, j, f"similar_address:{addr_i} vs {addr_j}")

    seen = set()
    components = []
    components_with_reasons = []

    for i in range(n):
        if i not in seen:
            stack = [i]
            comp_indices = []
            comp_reasons_for_graph = defaultdict(set)

            red_flag_attribute_strings = defaultdict(set)

            while stack:
                cur = stack.pop()
                if cur in seen:
                    continue
                seen.add(cur)
                comp_indices.append(cur)

                for neigh in graph[cur]:
                    if neigh not in seen:
                        stack.append(neigh)

                    pair = tuple(sorted((cur, neigh)))
                    if pair in edge_reasons:
                        comp_reasons_for_graph[pair].update(edge_reasons[pair])

                        for reason in edge_reasons[pair]:
                            if reason.startswith("shared_url:"):
                                red_flag_attribute_strings['url'].add(reason.split(':', 1)[1])
                            elif reason.startswith("shared_phone:"):
                                red_flag_attribute_strings['phone'].add(reason.split(':', 1)[1])
                            elif reason.startswith("similar_address:"):
                                addrs = reason.split(':', 1)[1].split(' vs ')
                                red_flag_attribute_strings['address'].add(addrs[0].strip())
                                red_flag_attribute_strings['address'].add(addrs[1].strip())

            if len(comp_indices) >= 2:
                firm_set = set(normalize_firm_name(awards[idx]["firm"]) for idx in comp_indices)
                if len(firm_set) > 1:
                    components_with_reasons.append((comp_indices, comp_reasons_for_graph, red_flag_attribute_strings))

    return components_with_reasons

# --- display_graph_for_component with styling and fit=True changes ---
def display_graph_for_component(awards, component_indices, component_reasons, red_flag_attribute_strings):
    nodes = []
    edges = []

    node_ids = set()

    # --- Node/Edge Colors and Sizes (Revised for Grey Scale and Prominent Red Stars) ---
    GREY_DARK = "#444444"
    GREY_MEDIUM = "#888888"
    GREY_LIGHT = "#BBBBBB"

    # Base colors (shades of grey)
    NODE_COLOR_FIRM = GREY_DARK
    NODE_COLOR_URL = GREY_MEDIUM
    NODE_COLOR_ADDRESS = GREY_MEDIUM
    NODE_COLOR_PHONE = GREY_MEDIUM

    # Highlight colors (Red for "red flags")
    HIGHLIGHT_COLOR_NODE = "#FF0000" # Bright Red for the star nodes
    HIGHLIGHT_COLOR_EDGE = "#FF0000" # Bright Red for the connecting edges
    HIGHLIGHT_EDGE_WIDTH = 3 # Thickness of red edges
    HIGHLIGHT_NODE_BORDER_COLOR = "#FF0000" # Red border for firms
    HIGHLIGHT_NODE_BORDER_WIDTH = 3 # Thickness of firm border
    HIGHLIGHT_NODE_SIZE_FACTOR = 1.8 # Make star nodes significantly larger

    # New style for the "SIMILAR_TO" address link
    SIMILAR_ADDRESS_EDGE_COLOR = "#FF0000" # Use the same red for consistency
    SIMILAR_ADDRESS_EDGE_WIDTH = 4 # Even thicker
    SIMILAR_ADDRESS_EDGE_DASHES = [10, 5] # Dashed line

    # --- Node Sizes (Adjusted for prominence) ---
    NODE_SIZE_FIRM = 35 # Slightly larger base for firms
    NODE_SIZE_ATTR_DEFAULT = 15 # Significantly smaller for non-red-flag attributes


    firm_node_map = {}
    address_node_id_map = {}

    # --- Identify firms that are part of a 'red flag' link directly (for firm node border) ---
    firms_in_red_flag_link = set()
    for (idx1, idx2), reasons in component_reasons.items():
        if reasons:
            firms_in_red_flag_link.add(normalize_firm_name(awards[idx1]["firm"]))
            firms_in_red_flag_link.add(normalize_firm_name(awards[idx2]["firm"]))


    for award_idx in component_indices:
        award = awards[award_idx]
        firm_name = normalize_firm_name(award.get("firm", "Unknown Firm"))

        firm_node_id_for_group = f"firm_node_{firm_name}"
        if firm_node_id_for_group not in node_ids:
            is_firm_red_flag = firm_name in firms_in_red_flag_link
            firm_border_width = HIGHLIGHT_NODE_BORDER_WIDTH if is_firm_red_flag else 1
            firm_border_color = HIGHLIGHT_NODE_BORDER_COLOR if is_firm_red_flag else "black"

            nodes.append(Node(id=firm_node_id_for_group, label=firm_name,
                              size=NODE_SIZE_FIRM,
                              color=NODE_COLOR_FIRM,
                              shape="dot", font={"size": 14},
                              borderWidth=firm_border_width, borderColor=firm_border_color))
            node_ids.add(firm_node_id_for_group)
            firm_node_map[firm_name] = firm_node_id_for_group

        current_firm_node_id = firm_node_map[firm_name]

        # Add URL node and edge
        company_url = (award.get("company_url") or "").strip()
        if company_url and company_url.lower() != "none":
            url_id = f"url_node_{company_url}"
            is_red_flag_url_attr = company_url in red_flag_attribute_strings['url']

            url_node_color = HIGHLIGHT_COLOR_NODE if is_red_flag_url_attr else NODE_COLOR_URL
            url_node_size = NODE_SIZE_ATTR_DEFAULT * HIGHLIGHT_NODE_SIZE_FACTOR if is_red_flag_url_attr else NODE_SIZE_ATTR_DEFAULT
            url_node_shape = "star" if is_red_flag_url_attr else "box"
            url_node_border_width = HIGHLIGHT_NODE_BORDER_WIDTH if is_red_flag_url_attr else 1
            url_node_border_color = HIGHLIGHT_COLOR_EDGE if is_red_flag_url_attr else "black"


            if url_id not in node_ids:
                nodes.append(Node(id=url_id, label=company_url, size=url_node_size, color=url_node_color, shape=url_node_shape, font={"size": 10}, # Smaller font for smaller nodes
                                  borderWidth=url_node_border_width, borderColor=url_node_border_color))
                node_ids.add(url_id)

            edge_color = {"color": HIGHLIGHT_COLOR_EDGE} if is_red_flag_url_attr else {"color": GREY_LIGHT} # Light grey for non-highlighted edges
            edge_width = HIGHLIGHT_EDGE_WIDTH if is_red_flag_url_attr else 1
            # Changed label to "url"
            edges.append(Edge(source=current_firm_node_id, target=url_id, label="url", type="arrow", color=edge_color, width=edge_width))


        # Add Address node and edge (Now includes full address for display)
        address1 = (award.get("address1") or "").strip()
        address2 = (award.get("address2") or "").strip()
        city = (award.get("city") or "").strip()
        state = (award.get("state") or "").strip()
        zip_code = (award.get("zip") or "").strip()

        # Concatenate address components for display
        full_address = f"{address1}"
        if address2:
            full_address += f", {address2}"
        if city or state or zip_code:
            full_address += "\n" # New line for city, state, zip
            if city:
                full_address += f"{city}"
            if state:
                if city: full_address += ", "
                full_address += f"{state}"
            if zip_code:
                if city or state: full_address += " "
                full_address += f"{zip_code}"
        
        # Only create an address node if address1 exists
        if address1 and address1.lower() != "none":
            # Use address1 for the address_id map, but full_address for the node label
            address_id = f"address_node_{address1}"
            address_node_id_map[address1] = address_id # Store for later linking based on address1

            is_red_flag_address_attr = address1 in red_flag_attribute_strings['address']

            address_node_color = HIGHLIGHT_COLOR_NODE if is_red_flag_address_attr else NODE_COLOR_ADDRESS
            address_node_size = NODE_SIZE_ATTR_DEFAULT * HIGHLIGHT_NODE_SIZE_FACTOR if is_red_flag_address_attr else NODE_SIZE_ATTR_DEFAULT
            address_node_shape = "star" if is_red_flag_address_attr else "hexagon"
            address_node_border_width = HIGHLIGHT_NODE_BORDER_WIDTH if is_red_flag_address_attr else 1
            address_node_border_color = HIGHLIGHT_COLOR_EDGE if is_red_flag_address_attr else "black"


            if address_id not in node_ids:
                nodes.append(Node(id=address_id, label=full_address, size=address_node_size, color=address_node_color, shape=address_node_shape, font={"size": 10}, # Smaller font
                                  borderWidth=address_node_border_width, borderColor=address_node_border_color))
                node_ids.add(address_id)

            edge_color = {"color": HIGHLIGHT_COLOR_EDGE} if is_red_flag_address_attr else {"color": GREY_LIGHT} # Light grey for non-highlighted edges
            edge_width = HIGHLIGHT_EDGE_WIDTH if is_red_flag_address_attr else 1
            # Changed label to "address"
            edges.append(Edge(source=current_firm_node_id, target=address_id, label="address", type="arrow", color=edge_color, width=edge_width))

        # Add Phone nodes and edges
        for field in ["poc_phone", "pi_phone"]:
            phone = (award.get(field) or "").strip()
            if phone and phone.lower() != "none":
                phone_id = f"phone_node_{phone}"
                is_red_flag_phone_attr = phone in red_flag_attribute_strings['phone']

                phone_node_color = HIGHLIGHT_COLOR_NODE if is_red_flag_phone_attr else NODE_COLOR_PHONE
                phone_node_size = NODE_SIZE_ATTR_DEFAULT * HIGHLIGHT_NODE_SIZE_FACTOR if is_red_flag_phone_attr else NODE_SIZE_ATTR_DEFAULT
                phone_node_shape = "star" if is_red_flag_phone_attr else "triangle"
                phone_node_border_width = HIGHLIGHT_NODE_BORDER_WIDTH if is_red_flag_phone_attr else 1
                phone_node_border_color = HIGHLIGHT_COLOR_EDGE if is_red_flag_phone_attr else "black"


                if phone_id not in node_ids:
                    nodes.append(Node(id=phone_id, label=phone, size=phone_node_size, color=phone_node_color, shape=phone_node_shape, font={"size": 10}, # Smaller font
                                      borderWidth=phone_node_border_width, borderColor=phone_node_border_color))
                    node_ids.add(phone_id)

                edge_color = {"color": HIGHLIGHT_COLOR_EDGE} if is_red_flag_phone_attr else {"color": GREY_LIGHT}
                edge_width = HIGHLIGHT_EDGE_WIDTH if is_red_flag_phone_attr else 1
                # Changed label to "phone"
                edges.append(Edge(source=current_firm_node_id, target=phone_id, label="phone", type="arrow", color=edge_color, width=edge_width))

    # --- Add SIMILAR_TO edges between addresses that caused duplicate flags ---
    added_similar_address_edges = set()

    for (idx1, idx2), reasons in component_reasons.items():
        award1 = awards[idx1]
        award2 = awards[idx2]
        addr1 = (award1.get("address1") or "").strip()
        addr2 = (award2.get("address1") or "").strip()

        if f"similar_address:{addr1} vs {addr2}" in reasons or f"similar_address:{addr2} vs {addr1}" in reasons:
            if addr1 in address_node_id_map and addr2 in address_node_id_map:
                node_id1 = address_node_id_map[addr1]
                node_id2 = address_node_id_map[addr2]

                edge_pair = tuple(sorted((node_id1, node_id2)))
                if edge_pair not in added_similar_address_edges and node_id1 != node_id2:
                    # Changed label to "similar"
                    edges.append(Edge(source=node_id1, target=node_id2, label="similar", type="arrow",
                                      color={"color": SIMILAR_ADDRESS_EDGE_COLOR},
                                      width=SIMILAR_ADDRESS_EDGE_WIDTH,
                                      dashes=SIMILAR_ADDRESS_EDGE_DASHES))
                    added_similar_address_edges.add(edge_pair)


    if not nodes:
        st.info("No nodes to display in this graph group.")
        return

    config = Config(
        width=800,
        height=500,
        directed=True,
        nodeHighlightBehavior=True,
        highlightColor="#F7A7A6",
        collapsible=True,
        node={"labelProperty": "label", "font": {"size": 12}},
        # IMPORTANT: Updated link font color to blue
        link={"labelProperty": "label", "renderLabel": True, "font": {"size": 10, "color": "#0000FF"}},
        physics={"enabled": True, "solver": "barnesHut", "barnesHut": {"gravitationalConstant": -1000, "centralGravity": 0.1, "springLength": 80, "springConstant": 0.05, "damping": 0.09, "avoidOverlap": 0.5}},
        fit=True, # ADDED: Fit graph to view on load
    )

    agraph(nodes=nodes, edges=edges, config=config)


def display_results(awards):
    components_data = find_duplicate_components(awards)
    if not components_data:
        st.write("No matching groups found where rows with different firm names share a common value.")
        return

    total_duplicates_amount = 0.0
    for comp_indices, _, _ in components_data:
        comp_rows = [awards[i] for i in comp_indices]
        group_total = 0.0
        for award in comp_rows:
            try:
                group_total += float(award.get("award_amount", 0))
            except ValueError:
                group_total += 0
        total_duplicates_amount += group_total
    duplicate_entities = sum(len(comp_indices) for comp_indices, _, _ in components_data)
    total_awards = len(awards)

    col1, col2, col3 = st.columns(3)
    col1.metric(label="Total Awards Analyzed", value=total_awards)
    col2.metric(label="Duplicate Entities", value=duplicate_entities)
    col3.metric(label="Total Award Amount for Duplicates", value=f"${total_duplicates_amount:,.2f}")

    st.markdown("---")
    st.header("Duplicate Groups (Knowledge Graph & Details)")

    for comp_index, (comp_indices, comp_reasons, red_flag_attribute_strings) in enumerate(components_data): # Unpack all three
        comp_rows = [awards[i] for i in comp_indices]
        distinct_firms = sorted(set(normalize_firm_name(row["firm"]) for row in comp_rows))

        # Directly display the header for the group
        st.markdown(f"## Group {comp_index + 1}: {', '.join(distinct_firms)}")
        st.markdown("---") # Separator before the graph

        # Display the Knowledge Graph directly
        display_graph_for_component(awards, comp_indices, comp_reasons, red_flag_attribute_strings)
        st.markdown("---") # Separator after the graph

        # Display the details table directly below the graph, optionally in an expander for compactness
        # Keeping this in an expander is usually fine, as tables are less prone to rendering issues
        # than interactive graph components. If you want ALL content visible without expanders,
        # remove this st.expander as well.
        with st.expander(f"Click to View Detailed Data for Group {comp_index+1}", expanded=False):
            st.markdown(f"#### Detailed Data for Group {comp_index + 1}")
            df = pd.DataFrame(sorted(comp_rows, key=lambda a: normalize_firm_name(a.get("firm", ""))))

            # Updated required_cols to include new address fields
            required_cols = ["firm", "company_url", "address1", "address2", "city", "state", "zip", "poc_phone", "pi_phone", "ri_poc_phone", "award_link", "agency", "branch", "award_amount"]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = "N/A"

            df["Link"] = df["award_link"].apply(
                lambda x: f'<a href="https://www.sbir.gov/awards/{x}" target="_blank">link</a>' if x and x != "N/A" else "N/A"
            )

            # Updated display_cols to include new address fields
            display_cols = ["firm", "company_url", "address1", "address2", "city", "state", "zip", "poc_phone", "pi_phone", "ri_poc_phone", "Link", "agency", "branch", "award_amount"]
            df_display = df[[col for col in display_cols if col in df.columns]]

            st.markdown(df_display.to_html(escape=False), unsafe_allow_html=True)

            group_total = 0.0
            for award in comp_rows:
                try:
                    group_total += float(award.get("award_amount", 0))
                except ValueError:
                    group_total += 0
            st.write(f"**Total Award Amount for this group:** ${group_total:,.2f}")

        st.markdown("---") # A final separator between groups

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
