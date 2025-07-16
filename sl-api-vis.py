import requests
from collections import defaultdict
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import streamlit as st
import pandas as pd
import json
from streamlit_agraph import agraph, Node, Edge, Config # Import for graph visualization
import folium # Import folium for mapping
from streamlit_folium import st_folium # Import for displaying folium maps in Streamlit


st.set_page_config(layout="wide", page_title="SBIR Awards Duplicate Finder")

BASE_URL = "https://api.www.sbir.gov/public/api/awards"

# Initialize session state for run_analysis and filter values if not already present
if 'run_analysis' not in st.session_state:
    st.session_state.run_analysis = False
if 'filter_year' not in st.session_state:
    st.session_state.filter_year = 2023 # Default value
if 'filter_agency' not in st.session_state:
    st.session_state.filter_agency = "DOD" # Default value
if 'filter_branch' not in st.session_state:
    st.session_state.filter_branch = "USAF" # Default value

# --- Cached Data Fetching Functions ---
@st.cache_data(ttl=3600) # Cache for 1 hour
def fetch_page(start, agency, year, rows, page_number):
    params = {"agency": agency, "rows": rows, "start": start}
    if year:
        params["year"] = year
    # Using print instead of st.sidebar.write inside cached function
    print(f"Requesting Page {page_number} | Start Offset: {start}")
    try:
        response = requests.get(BASE_URL, params=params)
    except Exception as e:
        print(f"Error fetching page {page_number}: {e}") # Using print
        return []
    if response.status_code != 200:
        print(f"Error: Unable to fetch data (Status Code: {response.status_code})") # Using print
        return []
    data = response.json()

    data_str = json.dumps(data, indent=2)
    print(f"First 200 characters of JSON response:\n{data_str[:200]}...")

    if isinstance(data, list):
        return data
    print("Unexpected response format, stopping pagination.") # Using print
    return []

@st.cache_data(ttl=3600) # Cache for 1 hour
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
                    # Using print instead of st.sidebar.write inside cached function
                    print(f"Page {curr_page}: Fetched {len(page_awards)} rows")
                    results.extend(page_awards)
            if batch_empty:
                break
            start += batch_size * rows
            page += batch_size
    print(f"\nTotal rows collected before filtering: {len(results)}") # Using print
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

@st.cache_data(ttl=3600) # Cache the duplicate finding logic
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


        # Add Address node and edge
        address = (award.get("address1") or "").strip()
        if address and address.lower() != "none":
            address_id = f"address_node_{address}"
            address_node_id_map[address] = address_id # Store for later linking

            is_red_flag_address_attr = address in red_flag_attribute_strings['address']

            address_node_color = HIGHLIGHT_COLOR_NODE if is_red_flag_address_attr else NODE_COLOR_ADDRESS
            address_node_size = NODE_SIZE_ATTR_DEFAULT * HIGHLIGHT_NODE_SIZE_FACTOR if is_red_flag_address_attr else NODE_SIZE_ATTR_DEFAULT
            address_node_shape = "star" if is_red_flag_address_attr else "hexagon"
            address_node_border_width = HIGHLIGHT_NODE_BORDER_WIDTH if is_red_flag_address_attr else 1
            address_node_border_color = HIGHLIGHT_COLOR_EDGE if is_red_flag_address_attr else "black"


            if address_id not in node_ids:
                nodes.append(Node(id=address_id, label=address, size=address_node_size, color=address_node_color, shape=address_node_shape, font={"size": 10}, # Smaller font
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

# --- New function to get coordinates from address ---
@st.cache_data(ttl=86400) # Cache coordinates for 24 hours
def get_coordinates(address, city, state):
    if not address or not city or not state:
        return None
    full_address = f"{address}, {city}, {state}"
    headers = {'User-Agent': 'SBIR_Duplicate_Finder/1.0 (your_email@example.com)'} # Replace with your actual email
    try:
        response = requests.get(f"https://nominatim.openstreetmap.org/search?q={full_address}&format=json&limit=1", headers=headers)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        if data:
            lat = float(data[0]['lat'])
            lon = float(data[0]['lon'])
            return lat, lon
    except requests.exceptions.RequestException as e:
        print(f"Error fetching coordinates for {full_address}: {e}") # Use print in cached function
    except ValueError:
        print(f"Could not parse coordinates for {full_address}") # Use print in cached function
    return None

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

        # --- Mapping Tool Integration ---
        st.subheader(f"Location Map for Group {comp_index + 1}")
        locations = []
        for award in comp_rows:
            address1 = (award.get("address1") or "").strip()
            city = (award.get("city") or "").strip()
            state = (award.get("state") or "").strip()
            firm_name = award.get("firm", "Unknown Firm")

            coords = get_coordinates(address1, city, state)
            if coords:
                locations.append({"firm": firm_name, "lat": coords[0], "lon": coords[1], "address": f"{address1}, {city}, {state}"})
        
        if locations:
            # Center map on the first location, or a default if no locations
            map_center = [locations[0]['lat'], locations[0]['lon']] if locations else [39.8283, -98.5795] # US center
            m = folium.Map(location=map_center, zoom_start=5)

            for loc in locations:
                folium.Marker(
                    location=[loc['lat'], loc['lon']],
                    popup=f"<b>{loc['firm']}</b><br>{loc['address']}",
                    tooltip=loc['firm']
                ).add_to(m)
            
            st_folium(m, width=800, height=400)
        else:
            st.info("No valid addresses found to display on the map for this group.")
        st.markdown("---") # Separator after the map

        # Display the details table directly below the graph, optionally in an expander for compactness
        with st.expander(f"Click to View Detailed Data for Group {comp_index+1}", expanded=False):
            st.markdown(f"#### Detailed Data for Group {comp_index + 1}")
            df = pd.DataFrame(sorted(comp_rows, key=lambda a: normalize_firm_name(a.get("firm", ""))))

            # Update required_cols to include city, state, zip
            required_cols = [
                "firm", "company_url", "address1", "address2",
                "city", "state", "zip", # <--- ADDED THESE
                "poc_phone", "pi_phone", "ri_poc_phone", "award_link",
                "agency", "branch", "award_amount"
            ]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = "N/A"

            df["Link"] = df["award_link"].apply(
                lambda x: f'<a href="https://www.sbir.gov/awards/{x}" target="_blank">link</a>' if x and x != "N/A" else "N/A"
            )

            # Update display_cols to include city, state, zip (adjust order as desired)
            display_cols = [
                "firm", "company_url", "address1", "address2",
                "city", "state", "zip", # <--- ADDED THESE
                "poc_phone", "pi_phone", "ri_poc_phone", "Link",
                "agency", "branch", "award_amount"
            ]
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
    # Retrieve current filter values from session state
    current_year = st.session_state.filter_year
    current_agency = st.session_state.filter_agency
    current_branch = st.session_state.filter_branch

    year = st.sidebar.number_input("Year", value=current_year, step=1, help="Year to fetch SBIR awards from.", key="input_year")
    agency = st.sidebar.text_input("Agency", current_agency, help="e.g., DOD, DOE, NIH. Case-insensitive.", key="input_agency")
    branch = st.sidebar.text_input("Branch (optional)", current_branch, help="e.g., USAF, Army, Navy. Leave blank for all branches within the agency.", key="input_branch")

    # Use a callback function for the button to set session state
    def set_run_analysis_true():
        st.session_state.run_analysis = True
        # Also update the filter values in session state when button is clicked
        st.session_state.filter_year = year
        st.session_state.filter_agency = agency
        st.session_state.filter_branch = branch

    st.sidebar.button("Run Analysis", on_click=set_run_analysis_true)

    # Detect if filter inputs have changed since the last "Run Analysis" click
    # If they have, reset run_analysis to False to prompt user to click "Run Analysis" again
    # This ensures that outdated results are not shown.
    if (year != st.session_state.filter_year or
        agency != st.session_state.filter_agency or
        branch != st.session_state.filter_branch):
        st.session_state.run_analysis = False


    if not st.session_state.run_analysis:
        st.info("Adjust the filters in the sidebar and click 'Run Analysis' to fetch data.")
    else:
        st.sidebar.write("---")
        st.sidebar.write("Starting data fetch...")

        with st.spinner('Fetching awards data... This might take a while for large datasets.'):
            # Pass values from session state for consistency
            awards = fetch_awards(agency=st.session_state.filter_agency, year=st.session_state.filter_year, rows=100)

        if not awards:
            st.warning("No awards data fetched. Please check the filters and try again.")
            st.session_state.run_analysis = False # Reset state if fetch fails
            return

        if st.session_state.filter_branch.strip():
            filtered_awards = [award for award in awards if award.get("branch", "").upper() == st.session_state.filter_branch.upper()]
            st.sidebar.write(f"Total rows after branch filtering ({st.session_state.filter_branch.upper()}): {len(filtered_awards)}")
        else:
            filtered_awards = awards
            st.sidebar.write(f"Total rows fetched: {len(filtered_awards)}")

        if not filtered_awards:
            st.warning("No awards found after applying branch filter. Try a different branch or leave it blank.")
            st.session_state.run_analysis = False # Reset state if no filtered awards
            return

        st.sidebar.write("Running duplicate analysis...")
        with st.spinner('Analyzing duplicates and building graph...'):
            display_results(filtered_awards)

        st.success("Analysis complete!")

if __name__ == "__main__":
    main()
