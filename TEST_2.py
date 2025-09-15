import streamlit as st
import requests
import json
import base64
import pandas as pd
import io
import re
from datetime import datetime, date, timedelta, time
from supabase import create_client, Client
import os

# ------------------------------------------------- CONFIGURATION FOR STREAMLIT LAYOUT -------------------------------------------------

st.set_page_config(
    page_title="DXC Runbook",
    layout="wide")

# ------------------------------------------------- CONNECTWISE API CONFIG -------------------------------------------------

def create_supabase_client():
    try:
        url = st.secrets.supabase.SUPABASE_URL
        key = st.secrets.supabase.SUPABASE_KEY
        return create_client(url, key)
    except KeyError as e:
        st.error(f"Missing Supabase credential in `secrets.toml`: {e}")
        return None
def get_connectwise_auth_headers():
    try:
        connectwise_secrets = st.secrets["connectwise"]
        companyId = connectwise_secrets["connectwise_company_id"]
        publicKey = connectwise_secrets["connectwise_public_key"]
        privateKey = connectwise_secrets["connectwise_private_key"]
        clientId = connectwise_secrets["connectwise_client_id"]
        base_url = connectwise_secrets.get("connectwise_url_base", "https://api-na.myconnectwise.net/v4_6_release/apis/3.0")
    except KeyError as e:
        st.error(f"Missing ConnectWise credential in `secrets.toml`: {e}")
        return None, None
    auth_string = f"{companyId}+{publicKey}:{privateKey}"
    encoded_auth_string = base64.b64encode(auth_string.encode("ascii")).decode("ascii")
    headers = {
        "Authorization": f"Basic {encoded_auth_string}",
        "clientId": clientId,
        "Accept": "application/vnd.connectwise.com+json",
        "Content-Type": "application/json"}
    return headers, base_url
def get_connectwise_boards(headers, base_url):
    if not headers or not base_url:
        return None
    url = f"{base_url}/service/boards"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTP Error fetching boards: {e}")
        st.error(f"Response content: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching ConnectWise boards: {e}")
        return None
def get_connectwise_tickets(headers, base_url, board_id=None, status_id=None, start_date=None, end_date=None):
    if not headers or not base_url:
        return None
    all_tickets = []
    page = 1
    page_size = 1000
    while True:
        url = f"{base_url}/service/tickets"
        conditions = []
        if board_id:
            conditions.append(f'board/id = {board_id}')
        if start_date and end_date:
            start_date_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
            end_date_str = end_date.strftime("%Y-%m-%dT23:59:59Z")
            conditions.append(f'dateEntered >= "{start_date_str}" and dateEntered <= "{end_date_str}"')
        elif start_date:
            start_date_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
            conditions.append(f'dateEntered >= "{start_date_str}"')
        elif end_date:
            end_date_str = end_date.strftime("%Y-%m-%dT23:59:59Z")
            conditions.append(f'dateEntered <= "{end_date_str}"')
        params = {
            "pageSize": page_size,
            "page": page}
        if conditions:
            params["conditions"] = " and ".join(conditions)
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status() 
            tickets = response.json()
            if not tickets:
                break
            all_tickets.extend(tickets)
            if len(tickets) < page_size:
                break
            page += 1
        except requests.exceptions.HTTPError as e:
            st.error(f"HTTP Error: {e}")
            st.error(f"Response content: {e.response.text}")
            return None
        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching ConnectWise tickets: {e}")
            return None
    return all_tickets
def get_connectwise_single_ticket(headers, base_url, ticket_id):
    if not headers or not base_url or not ticket_id:
        return None
    url = f"{base_url}/service/tickets/{ticket_id}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTP Error fetching ticket {ticket_id}: {e}")
        st.error(f"Response content: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching ConnectWise ticket {ticket_id}: {e}")
        return None
def get_connectwise_ticket_notes(headers, base_url, ticket_id):
    if not headers or not base_url or not ticket_id:
        return None
    url = f"{base_url}/service/tickets/{ticket_id}/notes"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTP Error fetching notes for ticket {ticket_id}: {e}")
        st.error(f"Response content: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching ConnectWise ticket notes: {e}")
        return None
def add_connectwise_ticket_note(headers, base_url, ticket_id, note_text):
    if not headers or not base_url or not ticket_id or not note_text:
        st.error("Missing parameters for adding a ticket note.")
        return None
    url = f"{base_url}/service/tickets/{ticket_id}/notes"
    note_payload = {
        "text": note_text,
        "detailDescriptionFlag": True,
        "internalAnalysisFlag": False,
        "resolutionFlag": False}
    try:
        response = requests.post(url, headers=headers, json=note_payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTP Error adding note to ticket {ticket_id}: {e}")
        st.error(f"Response content: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error adding ConnectWise ticket note: {e}")
        return None
def add_connectwise_resolution_note(headers, base_url, ticket_id, note_text):
    if not headers or not base_url or not ticket_id or not note_text:
        st.error("Missing parameters for adding a resolution note.")
        return None
    url = f"{base_url}/service/tickets/{ticket_id}/notes"
    note_payload = {
        "text": note_text,
        "detailDescriptionFlag": False,
        "internalAnalysisFlag": False,
        "resolutionFlag": True}
    try:
        response = requests.post(url, headers=headers, json=note_payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTP Error adding resolution note to ticket {ticket_id}: {e}")
        st.error(f"Response content: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error adding ConnectWise resolution note: {e}")
        return None
def flatten_ticket_data(tickets, headers, base_url):
    flattened_tickets = []
    hp_now_pattern = re.compile(r"HP Now Ticket #\s*([^\s\n]+)", re.IGNORECASE)
    custom_field_mapping = {
        'check-in': 'CW-Check-In (Custom Field)',
        'check-out': 'CW-Check-Out (Custom Field)',
        'total hours': 'CW-Total Hours (Custom Field)',
        'technician name': 'CW-Technician Name (Custom Field)',
        'description': 'CW-Description (Custom Field)'}
    two_hour_sites = ["AQN", "BOI", "COR", "PAL", "SDG"]
    for ticket in tickets:
        flattened_ticket = {}
        full_description = ''
        ticket_notes = get_connectwise_ticket_notes(headers, base_url, ticket['id'])
        if ticket_notes and len(ticket_notes) > 0 and 'text' in ticket_notes[0]:
            full_description = ticket_notes[0]['text']
        flattened_ticket['Full Description'] = full_description
        hp_now_ticket_value = None
        if full_description:
            match = hp_now_pattern.search(full_description)
            if match:
                hp_now_ticket_value = match.group(1).strip()
        flattened_ticket['HP Now Ticket #'] = hp_now_ticket_value
        for key, value in ticket.items():
            if isinstance(value, dict) and 'name' in value:
                flattened_ticket[key] = value['name']
            elif key == 'customFields' and isinstance(value, list):
                for custom_field in value:
                    if 'caption' in custom_field:
                        caption_lower = custom_field['caption'].lower()
                        found_match = False
                        for match_key, new_key in custom_field_mapping.items():
                            if match_key in caption_lower:
                                flattened_ticket[new_key] = custom_field.get('value', None)
                                found_match = True
                                break
                        if not found_match:
                            flattened_ticket[f"CW-{custom_field['caption']} (Custom Field)"] = custom_field.get('value', None)
            else:
                flattened_ticket[key] = value
        sla = "N/A"
        priority_name = flattened_ticket.get('priority')
        site_name_with_code = flattened_ticket.get('site')
        site_code = None
        if site_name_with_code and ' - ' in site_name_with_code:
            site_code = site_name_with_code.split(' - ')[-1].strip()
        two_hour_sites = ["AQN", "BOI", "COR", "PAL", "SDG"]
        if priority_name == "Priority 3 - Medium":
            sla = "2 Day"
        elif priority_name == "Priority 4 - Low":
            sla = "4 Day"
        elif priority_name in ["Priority 1 - Critical", "Priority 2 - High"]:
            if site_code and site_code in two_hour_sites:
                sla = "2 Hour"
            else:
                sla = "4 Hour"
        flattened_ticket['SLA'] = sla
        flattened_tickets.append(flattened_ticket)
    return flattened_tickets
def parse_cw_timestamp(timestamp_str):
    if not timestamp_str or not isinstance(timestamp_str, str):
        return None, None
    try:
        dt_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        return dt_obj.date(), dt_obj.strftime("%I:%M %p")
    except ValueError:
        return None, None
def get_company_by_name(headers, base_url, company_name):
    if not headers or not base_url or not company_name:
        return None
    url = f"{base_url}/company/companies"
    params = {
        "conditions": f'name = "{company_name}"'}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        companies = response.json()
        if companies:
            return companies[0]
        return None
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTP Error fetching company '{company_name}': {e}")
        st.error(f"Response content: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching ConnectWise company details: {e}")
        return None
def get_site_by_name(headers, base_url, company_id, site_name):
    if not headers or not base_url or not site_name or not company_id:
        return None
    url = f"{base_url}/company/companies/{company_id}/sites"
    params = {
        "conditions": f'name like "{site_name}"'}
    try:
        st.info(f"Searching for site name matching '{site_name}' within company ID {company_id}...")
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        sites = response.json()
        for site in sites:
            if site.get('name', '').lower().find(site_name.lower()) != -1:
                return site
        return None
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTP Error fetching site '{site_name}': {e}")
        st.error(f"Response content: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching ConnectWise site details: {e}")
        return None
def update_connectwise_ticket(headers, base_url, ticket_id, update_payload):
    if not headers or not base_url or not ticket_id or not update_payload:
        st.error("Missing parameters for ticket update.")
        return None
    url = f"{base_url}/service/tickets/{ticket_id}"
    try:
        response = requests.patch(url, headers=headers, json=update_payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTP Error updating ticket {ticket_id}: {e}")
        st.error(f"Response content: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error updating ConnectWise ticket {ticket_id}: {e}")
        return None
def get_technicians_by_site(site_code):
    supabase: Client = create_supabase_client()
    if not supabase:
        return None
    try:
        response_names_and_sites = supabase.table('names_and_sites').select('Name').eq('Site', site_code).eq('Badge', 'YES').execute()
        if not response_names_and_sites.data:
            return pd.DataFrame()
        badged_tech_names = [item['Name'] for item in response_names_and_sites.data]
        if not badged_tech_names:
            return pd.DataFrame()
        tech_data = []
        for full_name in badged_tech_names:
            parts = full_name.split()
            first_name = parts[0]
            last_name = parts[-1]
            response_tech_info = supabase.table('TECH INFORMATION').select('FIRST_NAME, LAST_NAME, PHONE_NUMBER, FIELD_NATION_ID, SURYL_EMAIL').eq('FIRST_NAME', first_name).eq('LAST_NAME', last_name).eq('SITE', site_code).execute()
            if response_tech_info.data:
                tech_data.append(response_tech_info.data[0])
        if tech_data:
            df = pd.DataFrame(tech_data)
            display_df = df[['FIRST_NAME', 'LAST_NAME', 'PHONE_NUMBER', 'FIELD_NATION_ID', 'SURYL_EMAIL']]
            return display_df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error querying Supabase: {e}")
        return None
def get_all_technicians():
    approved_technicians = [
        {'first_name': 'Mike', 'last_name': 'Sears'},
        {'first_name': 'Chaz', 'last_name': 'Crommartie'}]
    return approved_technicians
def calculate_multiplier(check_in_date: date, check_in_time: time) -> float:
    check_in_datetime = datetime.combine(check_in_date, check_in_time)
    day_of_week = check_in_datetime.weekday()
    hour = check_in_datetime.hour
    if day_of_week == 6:
        return 2.0
    elif day_of_week == 5:
        if 8 <= hour < 18:
            return 1.5
        else:
            return 2.0
    elif 0 <= day_of_week <= 4:
        if 8 <= hour < 18:
            return 1.0
        elif 18 <= hour:
            return 1.5
        else:
            return 2.0  
    return 1.0
def find_field_nation_internal_note(ticket_notes: list) -> str:
    if not ticket_notes:
        return "No Field Nation internal note found."
    for note in ticket_notes:
        if note.get('createdBy') == 'FieldNationAPI' and note.get('internalAnalysisFlag') is True:
            return note.get('text', "No text found in Field Nation internal note.")
    return "No Field Nation internal note found."
def extract_actions_taken(full_description: str) -> str:
    if not full_description:
        return "No provider notes found in the ticket's internal notes."
    pattern = re.compile(r".*Provider(?:'s|&#039;s|&#39;s)?\s+closing\s+notes:?\s*(.*)", re.IGNORECASE | re.DOTALL)
    match = pattern.search(full_description)
    if match:
        return match.group(1).strip()
    return "No provider notes found in the ticket's internal notes."
def update_ticket_dates(eta_string, ticket_id):
    try:
        parsed_datetime = datetime.strptime(eta_string.strip(), "%m/%d, %I:%M%p")
    except ValueError:
        try:
            parsed_datetime = datetime.strptime(eta_string.strip(), "%m/%d, %I%p")
        except ValueError:
            st.error("Invalid ETA format. Please use 'MM/DD, HPM' (e.g., '9/13, 1PM') or 'MM/DD, H:MM PM' (e.g., '8/12, 12:30PM').")
            return
    current_year = datetime.now().year
    start_datetime = parsed_datetime.replace(year=current_year)
    end_datetime = start_datetime + timedelta(hours=2)
    start_date_str = start_datetime.strftime("%Y-%m-%dT00:00:00Z")
    end_date_str = end_datetime.strftime("%Y-%m-%dT00:00:00Z")
    if start_datetime.minute == 0:
        start_time_str = start_datetime.strftime("%I%p").lstrip('0').lower()
    else:
        start_time_str = start_datetime.strftime("%I:%M%p").lstrip('0').lower()
    if end_datetime.minute == 0:
        end_time_str = end_datetime.strftime("%I%p").lstrip('0').lower()
    else:
        end_time_str = end_datetime.strftime("%I:%M%p").lstrip('0').lower()
    payload = [
            {"op": "replace", "path": "customFields", "value": [
            {"id": 9, "caption": "Start Date of Request", "value": start_date_str},
            {"id": 10, "caption": "Start Time of Request", "value": start_time_str},
            {"id": 11, "caption": "End Date of Request", "value": end_date_str},
            {"id": 12, "caption": "End Time of Request", "value": end_time_str}]}]
    auth_headers, base_url = get_connectwise_auth_headers()
    ticket_id = str(ticket_id)
    if auth_headers and base_url:
        try:
            response = requests.patch(f"{base_url}/service/tickets/{ticket_id}", json=payload, headers=auth_headers)
            response.raise_for_status()
            st.success(f"Ticket {ticket_id} scheduling details updated successfully!")
        except requests.exceptions.RequestException as e:
            st.error(f"Failed to update ticket {ticket_id} scheduling details: {e}")
    else:
        st.error("Failed to get ConnectWise authentication headers.")
def get_status_by_name(auth_headers, base_url, board_id, status_name):
    try:
        url = f"{base_url}/service/boards/{board_id}/statuses"
        response = requests.get(url, headers=auth_headers)
        response.raise_for_status()
        statuses = response.json()
        for status in statuses:
            if status.get('name', '').lower() == status_name.lower():
                return status
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch statuses for board {board_id}: {e}")
        return None
def update_connectwise_ticket_status(auth_headers, base_url, ticket_id, status_object):
    try:
        url = f"{base_url}/service/tickets/{ticket_id}"
        payload = [
            {"op": "replace", "path": "status", "value": status_object}]
        response = requests.patch(url, json=payload, headers=auth_headers)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to update ticket status for ticket {ticket_id}: {e}")
        return False

# ------------------------------------------------- PAGE FUNCTIONS -------------------------------------------------

# ------------------------------------------------- LANDING PAGE -------------------------------------------------

def landing_page():
    st.title("DXC Runbook and Ticket Reporting System")
    st.write("Welcome to the SURYL - DXC Hub")

# ------------------------------------------------- CW TICKET REPORT -------------------------------------------------

def connectwise_page():
    st.title("ConnectWise API Integration")
    st.markdown("This page connects to the ConnectWise API to fetch and display ticket information.")
    auth_headers, base_url = get_connectwise_auth_headers()
    if auth_headers and base_url:
        if "boards" not in st.session_state:
            with st.spinner("Fetching service boards..."):
                boards_data = get_connectwise_boards(auth_headers, base_url)
                if boards_data:
                    st.session_state["boards"] = {board["name"]: board["id"] for board in boards_data}
                else:
                    st.session_state["boards"] = {}
        dxc_board_id = None
        if st.session_state["boards"]:
            dxc_board_name = "DXCSupport"
            if dxc_board_name in st.session_state["boards"]:
                dxc_board_id = st.session_state["boards"][dxc_board_name]
            else:
                st.warning(f"Could not find a board named '{dxc_board_name}'.")
        else:
            st.warning("Could not fetch a list of service boards. Please check your API credentials.")
        with st.form("single_ticket_form"):
            st.subheader("Fetch a Single Ticket")
            ticket_id_input = st.text_input("Enter a specific ticket ID:", "")
            submit_single = st.form_submit_button("Fetch Single Ticket")
        if submit_single and ticket_id_input:
            with st.spinner(f"Fetching single ticket {ticket_id_input}..."):
                ticket_data = get_connectwise_single_ticket(auth_headers, base_url, ticket_id_input)
                if ticket_data:
                    st.session_state["tickets"] = [ticket_data]
                    st.session_state["flattened_tickets"] = flatten_ticket_data([ticket_data], auth_headers, base_url)
                    st.success(f"Ticket {ticket_id_input} fetched successfully!")
                    st.subheader(f"Raw JSON for Ticket {ticket_id_input}")
                    st.json(ticket_data)
                    st.subheader("Description Fields from Raw JSON")
                    with st.expander("Click to view full description text"):
                        st.write("### Full Description (from ticket notes)")
                        st.text_area("Full Description", st.session_state["flattened_tickets"][0].get('Full Description', 'Not found'), height=300)
                        st.write("### CW-Description (Custom Field)")
                        custom_description_value = "Not found"
                        if 'customFields' in ticket_data and isinstance(ticket_data['customFields'], list):
                            for custom_field in ticket_data['customFields']:
                                if custom_field.get('caption') == 'Description':
                                    custom_description_value = custom_field.get('value', 'Not found')
                                    break
                        st.text_area("Custom Field 'Description'", custom_description_value, height=300)
                else:
                    st.session_state["tickets"] = []
                    st.session_state["flattened_tickets"] = []
                    st.error(f"Could not fetch ticket with ID: {ticket_id_input}")
        st.markdown("---")
        st.subheader("DXCSupport Board Ticket Reporting")
        if dxc_board_id:
            today = date.today()
            last_week = today - timedelta(days=7)
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", value=last_week)
            with col2:
                end_date = st.date_input("End Date", value=today)
            if st.button("Fetch DXCSupport Tickets"):
                with st.spinner(f"Fetching tickets from {start_date} to {end_date} for DXCSupport Board..."):
                    tickets = get_connectwise_tickets(
                        auth_headers, 
                        base_url, 
                        board_id=dxc_board_id, 
                        start_date=start_date,
                        end_date=end_date)
                if tickets:
                    st.session_state["tickets"] = tickets
                    st.session_state["flattened_tickets"] = flatten_ticket_data(tickets, auth_headers, base_url)
                    st.success(f"Tickets fetched successfully for 'DXCSupport Board'!")
                    st.write(f"Found {len(st.session_state['flattened_tickets'])} tickets.")
                    df = pd.DataFrame(st.session_state["flattened_tickets"])
                    for col in ['CW-Check-In (Custom Field)', 'CW-Check-Out (Custom Field)']:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                    if 'CW-Check-In (Custom Field)' in df.columns:
                        df['Check in Date'] = df['CW-Check-In (Custom Field)'].dt.date
                        df['Check in Time'] = df['CW-Check-In (Custom Field)'].dt.time
                    if 'CW-Check-Out (Custom Field)' in df.columns:
                        df['Check Out Date'] = df['CW-Check-Out (Custom Field)'].dt.date
                        df['Check Out Time'] = df['CW-Check-Out (Custom Field)'].dt.time
                    columns_to_keep = [
                        ('HP Now Ticket #', 'HP Now Ticket #'),
                        ('id', 'Suryl Ticket #'),
                        ('summary', 'Summary'),
                        ('site', 'Site Name'),
                        ('siteName', 'Site Name'),
                        ('status', 'Status'),
                        ('type', 'Type'),
                        ('subType', 'SubType'),
                        ('item', 'Itam'),
                        ('priority', 'Priority'),
                        ('CW-Technician Name (Custom Field)', 'Technician Name'),
                        ('Check in Date', 'Check In Date'),
                        ('Check in Time', 'Check In Time'),
                        ('Check Out Date', 'Check Out Date'),
                        ('Check Out Time', 'Check Out Time'),
                        ('CW-Total Hours (Custom Field)', 'Total Hours')]
                    filtered_data = []
                    for _, row in df.iterrows():
                        new_row = {}
                        for old_key, new_key in columns_to_keep:
                            new_row[new_key] = row.get(old_key)
                        filtered_data.append(new_row)
                    df_selected = pd.DataFrame(filtered_data)
                    df_for_excel = df_selected.copy()
                    if 'Check in Time' in df_selected.columns:
                        df_selected['Check in Time'] = df_selected['Check in Time'].apply(lambda x: x.strftime('%I:%M %p') if pd.notna(x) else None)
                    if 'Check Out Time' in df_selected.columns:
                        df_selected['Check Out Time'] = df_selected['Check Out Time'].apply(lambda x: x.strftime('%I:%M %p') if pd.notna(x) else None)
                    st.dataframe(df_selected)
                    st.markdown("---")
                    st.header("Export Tickets to Excel")
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df_for_excel.to_excel(writer, index=False, sheet_name='Tickets')
                    output.seek(0)
                    st.download_button(
                        label="Download Excel File",
                        data=output,
                        file_name="dxc_connectwise_tickets.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                else:
                    st.session_state["tickets"] = []
                    st.session_state["flattened_tickets"] = []
                    st.warning("No tickets found for the selected date range or an error occurred.")

# ------------------------------------------------- RUNBOOK PAGE -------------------------------------------------

# ------------------------------------------------- RUNBOOK PAGE -------------------------------------------------

def runbook_page():
    st.title("DXC Runbook")
    st.write("This page will contain the Runbook content.")
    if 'current_ticket_id' not in st.session_state:
        st.session_state.current_ticket_id = None
        st.session_state.new_site_name = None
        st.session_state.site_change_initiated = False
        st.session_state.current_ticket_data = None
        st.session_state.company_id = None
        st.session_state.tech_df = None

    st.header("Search for a Ticket")
    with st.form("runbook_ticket_search_form"):
        ticket_id_input = st.text_input("Enter a specific ticket ID:", value=st.session_state.current_ticket_id or "")
        search_button = st.form_submit_button("Search Ticket")

    if search_button and ticket_id_input:
        st.session_state.site_change_initiated = False
        st.session_state.current_ticket_id = ticket_id_input
        st.info(f"Searching for ticket: {st.session_state.current_ticket_id}...")
        auth_headers, base_url = get_connectwise_auth_headers()
        if auth_headers and base_url:
            with st.spinner(f"Fetching ticket {st.session_state.current_ticket_id}..."):
                ticket_data = get_connectwise_single_ticket(auth_headers, base_url, st.session_state.current_ticket_id)
            if ticket_data:
                st.session_state.current_ticket_data = ticket_data
                company_name = ticket_data.get('company', {}).get('name')
                if company_name:
                    with st.spinner(f"Fetching company details for '{company_name}'..."):
                        company_details = get_company_by_name(auth_headers, base_url, company_name)
                    if company_details:
                        st.session_state.company_id = company_details.get('id')
                    else:
                        st.error(f"Could not find company details for '{company_name}'.")
                else:
                    st.error("Ticket data does not contain a company name.")
            else:
                st.session_state.current_ticket_data = None
                st.error(f"Could not find ticket with ID: {st.session_state.current_ticket_id}.")
    
    if st.session_state.current_ticket_data:
        ticket_data = st.session_state.current_ticket_data
        site_name = ticket_data.get('site', {}).get('name')
        priority_name = ticket_data.get('priority', {}).get('name')
        auth_headers, base_url = get_connectwise_auth_headers()
        ticket_notes = get_connectwise_ticket_notes(auth_headers, base_url, st.session_state.current_ticket_id)
        full_description = ''
        if ticket_notes and len(ticket_notes) > 0 and 'text' in ticket_notes[0]:
            full_description = ticket_notes[0]['text']

        with st.expander("View Full Ticket Description"):
            st.text_area("Ticket Notes", full_description, height=300)

        st.subheader("Ticket Details")
        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown(f"### **Site:**")
            st.write(f"{site_name}")
            if site_name == "Additional Site":
                sites_continued_pattern = re.compile(r"Sites Continued:?\s*(.*)", re.IGNORECASE)
                match = sites_continued_pattern.search(full_description)
                new_site_name = None
                if match:
                    new_site_name = match.group(1).strip()
                if new_site_name:
                    st.markdown(f"The ticket description contains a new site name.")
                    st.markdown(f"**Current Site:** `Additional Site`")
                    st.session_state.new_site_name = st.text_input("New Site Name:", value=st.session_state.new_site_name or new_site_name)

                st.markdown("---")
                with st.form("site_change_form"):
                    if new_site_name:
                        proceed_button = st.form_submit_button("Proceed with Site Change")
                    else:
                        st.info("No new site name found in description.")
                        proceed_button = False

                    if proceed_button:
                        st.session_state.site_change_initiated = True

        with col2:
            st.markdown(f"### **Priority:**")
            st.write(f"{priority_name}")
            st.markdown(f"### **Scheduling Details**")
            if 'customFields' in ticket_data and isinstance(ticket_data['customFields'], list):
                custom_fields_dict = {cf['caption']: cf.get('value') for cf in ticket_data['customFields']}
                start_date_str = custom_fields_dict.get('Start Date of Request', None)
                end_date_str = custom_fields_dict.get('End Date of Request', None)
                start_date_display = start_date_str.split('T')[0] if start_date_str and 'T' in start_date_str else start_date_str
                end_date_display = end_date_str.split('T')[0] if end_date_str and 'T' in end_date_str else end_date_str
                start_time = custom_fields_dict.get('Start Time of Request', None)
                end_time = custom_fields_dict.get('End Time of Request', None)
                end_time_display = end_time
                if start_date_str and end_date_str and start_time and not end_time:
                    end_time_display = start_time

                st.markdown(f"**Start Date of Request:** {start_date_display if start_date_display else 'None'}")
                st.markdown(f"**Start Time of Request:** {start_time if start_time else 'None'}")
                st.markdown(f"**End Date of Request:** {end_date_display if end_date_display else 'None'}")
                st.markdown(f"**End Time of Request:** {end_time_display if end_time_display else 'None'}")
            else:
                st.info("No custom fields found for this ticket.")

        with col3:
            st.markdown("### **Scheduling Window**")
            if 'customFields' in ticket_data and isinstance(ticket_data['customFields'], list):
                custom_fields_dict = {cf['caption']: cf.get('value') for cf in ticket_data['customFields']}
                start_date_str = custom_fields_dict.get('Start Date of Request', None)
                start_time = custom_fields_dict.get('Start Time of Request', None)
                end_date_str = custom_fields_dict.get('End Date of Request', None)
                end_time = custom_fields_dict.get('End Time of Request', None)
                start_date_obj = datetime.strptime(start_date_str.split('T')[0], "%Y-%m-%d").date() if start_date_str else None
                end_date_obj = datetime.strptime(end_date_str.split('T')[0], "%Y-%m-%d").date() if end_date_str else None
                start_date_display = start_date_str.split('T')[0] if start_date_str else 'None'
                end_date_display = end_date_str.split('T')[0] if end_date_str else 'None'
                
                if start_date_obj and end_date_obj and start_date_obj == end_date_obj:
                    st.markdown(f"**Type:** Hard Start")
                    st.markdown(f"The activity is a **hard start** for {start_date_display} at {start_time}.")
                elif start_date_str and start_time and end_date_str and end_time:
                    st.markdown(f"**Type:** Schedulable Window")
                    st.markdown(f"The activity can be scheduled between {start_date_display} at {start_time} and {end_date_display} at {end_time}.")
                elif start_date_str and start_time and end_date_str and not end_time:
                    st.markdown(f"**Type:** Schedulable Window")
                    st.markdown(f"The activity can be scheduled between {start_date_display} at {start_time} and {end_date_display} at {start_time}.")
                elif start_date_str and start_time and not end_date_str and not end_time:
                    st.markdown(f"**Type:** Hard Start")
                    st.markdown(f"The activity is a **hard start** for {start_date_display} at {start_time}.")
                elif priority_name in ["1 - Critical", "2 - High"] and not start_date_str and not start_time:
                    st.markdown(f"**Type:** Hard Start (Deadline)")
                    if end_date_str and end_time:
                        st.markdown(f"Tech must be on site **before** {end_date_display} at {end_time}.")
                    else:
                        st.warning("Critical/High priority ticket with no clear deadline specified.")
                else:
                    st.info("No scheduling window details found.")
            else:
                st.info("No custom fields found for this ticket.")
        st.markdown("---")
        st.subheader("Available Badged Technicians")
        site_name_from_ticket = ticket_data.get('site', {}).get('name')
        site_code = site_name_from_ticket.split(' - ')[-1].strip() if site_name_from_ticket and ' - ' in site_name_from_ticket else site_name_from_ticket
        if site_code and site_code != 'Additional Site':
            with st.spinner(f"Looking up badged technicians for site '{site_code}'..."):
                tech_df = get_technicians_by_site(site_code)
                st.session_state.tech_df = tech_df
            if tech_df is not None and not tech_df.empty:
                st.dataframe(tech_df.drop('SURYL_EMAIL', axis=1), hide_index=True)
            else:
                st.info(f"No badged technicians found for site code '{site_code}'.")
        else:
            st.warning("Could not determine a site code from the ticket.")
            st.session_state.tech_df = None
        if st.session_state.tech_df is not None and not st.session_state.tech_df.empty:
            st.markdown("---")
            st.subheader("Send Discussion Note")
            with st.form("discussion_note_form"):
                tech_names = [f"{row['FIRST_NAME']} {row['LAST_NAME']}" for _, row in st.session_state.tech_df.iterrows()]
                selected_tech_name = st.selectbox("Select Technician:", options=tech_names)
                eta = st.text_input("Enter ETA (e.g., '7/24, 8AM'):")
                send_note_button = st.form_submit_button("Send Note & Update Ticket")
                if send_note_button:
                    if not eta:
                        st.error("Please enter an ETA.")
                    else:
                        try:
                            parsed_datetime = datetime.strptime(eta.strip(), "%m/%d, %I:%M%p")
                        except ValueError:
                            try:
                                parsed_datetime = datetime.strptime(eta.strip(), "%m/%d, %I%p")
                            except ValueError:
                                st.error("Invalid ETA format. Please use 'MM/DD, HPM' (e.g., '9/13, 1PM') or 'MM/DD, H:MM PM' (e.g., '8/12, 12:30PM').")
                                return
                        current_year = datetime.now().year
                        start_datetime = parsed_datetime.replace(year=current_year)
                        new_date_str = start_datetime.strftime("%A, %B %d, %Y")
                        new_date_str = new_date_str.replace(" 0", " ")                        
                        current_summary = st.session_state.current_ticket_data.get('summary', '')
                        date_pattern = re.compile(r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), (January|February|March|April|May|June|July|August|September|October|November|December) \d+, \d{4}")
                        match = date_pattern.search(current_summary)
                        if match:
                            new_summary = date_pattern.sub(new_date_str, current_summary)
                        else:
                            new_summary = f"{current_summary} {new_date_str}"
                        summary_payload = [{"op": "replace", "path": "summary", "value": new_summary}]
                        with st.spinner("Updating ticket summary..."):
                            summary_update_result = update_connectwise_ticket(auth_headers, base_url, st.session_state.current_ticket_id, summary_payload)
                            if summary_update_result:
                                st.success("Ticket summary updated successfully!")
                            else:
                                st.error("Failed to update the ticket summary.")                        
                        selected_tech = st.session_state.tech_df.loc[
                            (st.session_state.tech_df['FIRST_NAME'] + ' ' + st.session_state.tech_df['LAST_NAME']) == selected_tech_name].iloc[0]
                        full_name = f"{selected_tech['FIRST_NAME']} {selected_tech['LAST_NAME']}"
                        sury_email = selected_tech['SURYL_EMAIL']
                        tech_id_to_send = selected_tech['FIELD_NATION_ID']
                        
                        note_text = (
                            f"Name: {full_name}\n"
                            f"Mail: {sury_email}\n"
                            f"ETA: {eta}")
                        with st.spinner("Adding discussion note to ticket..."):
                            note_result = add_connectwise_ticket_note(auth_headers, base_url, st.session_state.current_ticket_id, note_text)
                        
                        if note_result:
                            st.success(f"Discussion note added successfully to ticket {st.session_state.current_ticket_id}!")
                            
                            tech_id_payload = [
                                {"op": "replace", "path": "customFields", "value": [
                                {"id": 23, "caption": "Tech ID", "value": tech_id_to_send}]}]
                            
                            with st.spinner("Updating ticket with Tech ID..."):
                                tech_id_update_result = update_connectwise_ticket(auth_headers, base_url, st.session_state.current_ticket_id, tech_id_payload)
                                if tech_id_update_result:
                                    st.success(f"Tech ID updated successfully!")
                                else:
                                    st.error("Failed to update the Tech ID.")

                            with st.spinner("Updating ticket scheduling details..."):
                                update_ticket_dates(eta, st.session_state.current_ticket_id)
                            
                            with st.spinner("Updating ticket status to Dispatched..."):
                                board_id = st.session_state.current_ticket_data['board']['id']
                                dispatched_status_object = get_status_by_name(auth_headers, base_url, board_id, "Dispatched")
                                if dispatched_status_object:
                                    status_update_result = update_connectwise_ticket_status(auth_headers, base_url, st.session_state.current_ticket_id, dispatched_status_object)
                                    if status_update_result:
                                        st.success(f"Ticket status updated to 'Dispatched'!")
                                    else:
                                        st.error("Failed to update the ticket status.")
                                else:
                                    st.error("Could not find 'Dispatched' status on this board.")
                            st.rerun()
                        else:
                            st.error("Failed to add discussion note to ticket.")
    
    if st.session_state.site_change_initiated and st.session_state.current_ticket_data:
        auth_headers, base_url = get_connectwise_auth_headers()
        if auth_headers and base_url and st.session_state.new_site_name and st.session_state.company_id:
            st.info("Searching for the correct site in ConnectWise...")
            site_details = get_site_by_name(auth_headers, base_url, st.session_state.company_id, st.session_state.new_site_name)
            if site_details:
                site_id = site_details.get('id')
                st.info(f"Found site: '{site_details['name']}' (ID: {site_id}). Now updating ticket {st.session_state.current_ticket_id}...")
                update_payload = [
                    {"op": "replace", "path": "site", "value": {
                    "id": site_id,
                    "name": site_details['name']}}]
                with st.spinner("Submitting site change to ConnectWise..."):
                    updated_ticket = update_connectwise_ticket(auth_headers, base_url, st.session_state.current_ticket_id, update_payload)
                if updated_ticket:
                    st.success(f"Ticket **{st.session_state.current_ticket_id}** updated successfully! Reloading page to show changes.")
                    st.session_state.new_site_name = None
                    st.session_state.site_change_initiated = False
                    st.rerun()
                else:
                    st.error("Failed to update the ticket.")
            else:
                st.error(f"Could not find a site in ConnectWise with the name: '{st.session_state.new_site_name}'.")
        else:
            st.error("Site change could not be initiated due to missing data.")
        st.session_state.site_change_initiated = False
    else:
        pass

# ------------------------------------------------- TICKET INPUT PAGE -------------------------------------------------   

def input_tickets_page():
    st.title("Input Tickets and Log Data")
    st.write("Enter a ConnectWise ticket ID to pre-fill the form, then submit the data to the `live_dispatches` table.")
    if 'input_ticket_id' not in st.session_state:
        st.session_state.input_ticket_id = ""
    if 'ticket_form_data' not in st.session_state:
        st.session_state.ticket_form_data = None
    if 'actions_taken' not in st.session_state:
        st.session_state.actions_taken = ""
    auth_headers, base_url = get_connectwise_auth_headers()
    supabase = create_supabase_client()
    hardcoded_technicians = get_all_technicians()
    with st.form("search_ticket_form"):
        col1, col2 = st.columns([3, 1])
        with col1:
            ticket_id_to_search = st.text_input("Enter ConnectWise Ticket ID", value=st.session_state.input_ticket_id)
        with col2:
            st.markdown("##")
            fetch_button = st.form_submit_button("Fetch Details")
    if fetch_button and ticket_id_to_search:
        with st.spinner(f"Fetching details for ticket {ticket_id_to_search}..."):
            ticket_data = get_connectwise_single_ticket(auth_headers, base_url, ticket_id_to_search)
            ticket_notes = get_connectwise_ticket_notes(auth_headers, base_url, ticket_id_to_search)
            if ticket_data and ticket_notes:
                flattened_ticket_list = flatten_ticket_data([ticket_data], auth_headers, base_url)
                if flattened_ticket_list:
                    flattened_ticket = flattened_ticket_list[0]
                    hp_now_ticket_number = flattened_ticket.get('HP Now Ticket #', 'N/A')
                    sla_calculated = flattened_ticket.get('SLA', 'N/A')
                    technician_name = flattened_ticket.get('CW-Technician Name (Custom Field)', '').strip()
                    total_hours_str = flattened_ticket.get('CW-Total Hours (Custom Field)')
                    hours_from_cw = 0.0
                    if total_hours_str:
                        try:
                            hours_from_cw = float(total_hours_str)
                        except ValueError:
                            hours_from_cw = 0.0
                    check_in_cw_str = flattened_ticket.get('CW-Check-In (Custom Field)')
                    check_out_cw_str = flattened_ticket.get('CW-Check-Out (Custom Field)')
                    check_in_date_obj, check_in_time_str = parse_cw_timestamp(check_in_cw_str)
                    check_out_date_obj, check_out_time_str = parse_cw_timestamp(check_out_cw_str)
                    def safe_time_parse(time_str):
                        if not time_str:
                            return None
                        formats = ["%I:%M %p", "%H:%M"]
                        for fmt in formats:
                            try:
                                return datetime.strptime(time_str, fmt).time()
                            except ValueError:
                                continue
                        return None
                    check_in_time_obj = safe_time_parse(check_in_time_str) if check_in_time_str else None
                    multiplier_calculated = 1.0
                    if check_in_date_obj and check_in_time_obj:
                        multiplier_calculated = calculate_multiplier(check_in_date_obj, check_in_time_obj)
                    actions_taken = extract_actions_taken(find_field_nation_internal_note(ticket_notes))
                else:
                    hp_now_ticket_number = 'N/A'
                    technician_name = ''
                    sla_calculated = 'N/A'
                    hours_from_cw = 0.0
                    check_in_date_obj, check_in_time_str = None, None
                    check_out_date_obj, check_out_time_str = None, None
                    multiplier_calculated = 1.0
                    actions_taken = "No provider notes found in the ticket's internal notes."
                st.success(f"Ticket {ticket_id_to_search} details fetched successfully.")
                st.session_state.input_ticket_id = ticket_id_to_search
                site_name = ticket_data.get('site', {}).get('name', 'N/A')
                priority = ticket_data.get('priority', {}).get('name', 'N/A')
                st.session_state.ticket_form_data = {
                    'HPID': hp_now_ticket_number,
                    'SURYLID': str(ticket_id_to_search),
                    'Site': site_name,
                    'Priority': priority,
                    'Date': date.today(),
                    'Tech': technician_name,
                    'SLA': sla_calculated,
                    'Hours': hours_from_cw,
                    'CheckInDate': check_in_date_obj if check_in_date_obj else date.today(),
                    'CheckInTime': check_in_time_str if check_in_time_str else "09:00 AM",
                    'CheckOutDate': check_out_date_obj,
                    'CheckOutTime': check_out_time_str,
                    'Multiplier': multiplier_calculated}
                st.session_state.actions_taken = actions_taken
            else:
                st.error(f"Could not find ticket with ID: {ticket_id_to_search} or notes. Please try again.")
                st.session_state.ticket_form_data = None
                st.session_state.actions_taken = "No provider notes found in the ticket's internal notes."
    if st.session_state.ticket_form_data:
        st.markdown("---")
        st.subheader(f"Log Data for ConnectWise Ticket {st.session_state.input_ticket_id}")
        with st.form("combined_log_and_note_form"):
            form_data = st.session_state.ticket_form_data
            col1, col2, col3 = st.columns(3)
            with col1:
                st.text_input("ConnectWise Ticket ID (SURYLID)", value=form_data['SURYLID'], disabled=True)
                st.text_input("HP Now Ticket # (HPID)", value=form_data['HPID'], disabled=True)
                st.text_input("Site", value=form_data['Site'], disabled=True)
                st.text_input("Priority", value=form_data['Priority'], disabled=True)
                st.text_input("SLA", value=form_data['SLA'], disabled=True)
                hours = st.number_input("Hours", value=float(form_data['Hours']), min_value=0.0, step=0.5)
                multiplier = st.number_input("Multiplier", value=float(form_data['Multiplier']), min_value=1.0, step=0.5)
            with col2:
                prefilled_tech = form_data.get('Tech')
                if prefilled_tech and prefilled_tech.strip():
                    tech_options = [prefilled_tech]
                    selected_tech_name = st.selectbox("Technician", options=tech_options, index=0, disabled=True)
                else:
                    tech_options = [f"{tech['first_name']} {tech['last_name']}" for tech in hardcoded_technicians]
                    selected_tech_name = st.selectbox("Technician", options=tech_options, index=0)
                check_in_date = st.date_input("Check-In Date", value=form_data['CheckInDate'])
                check_in_time_str = st.text_input("Check-In Time (HH:MM AM/PM)", value=form_data['CheckInTime'])
            with col3:
                today_date = st.date_input("Date", value=form_data['Date'])
                check_out_date = st.date_input("Check-Out Date", value=form_data['CheckOutDate'] if form_data['CheckOutDate'] else None)
                check_out_time_str = st.text_input("Check-Out Time (HH:MM AM/PM)", value=form_data['CheckOutTime'] if form_data['CheckOutTime'] else "")
            st.markdown("---")
            st.subheader("Resolution Note for Customer")
            note_content = (
                f"Date of Visit: {check_in_date.strftime('%Y-%m-%d')}\n"
                f"Start Time: {check_in_time_str}\n"
                f"End Time: {check_out_time_str}\n"
                f"Time in Hours: {hours}\n"
                f"End User Notified: Yes\n\n"
                f"Actions Taken:\n"
                f"{st.session_state.actions_taken}")
            edited_note = st.text_area("Resolution Note for Customer:", value=note_content, height=300)
            submit_combined_button = st.form_submit_button("Submit & Send Note")
            if submit_combined_button:
                if not selected_tech_name:
                    st.error("Please select a technician.")
                    st.stop()
                if not edited_note:
                    st.error("The resolution note cannot be empty.")
                    st.stop()
                data_to_insert = {
                    'Date': today_date.isoformat(),
                    'Tech': selected_tech_name,
                    'SLA': form_data['SLA'],
                    'Site': form_data['Site'],
                    'Hours': hours,
                    'CheckInDate': check_in_date.isoformat() if check_in_date else None,
                    'CheckInTime': check_in_time_str,
                    'CheckOutDate': check_out_date.isoformat() if check_out_date else None,
                    'CheckOutTime': check_out_time_str,
                    'HPID': form_data['HPID'],
                    'SURYLID': form_data['SURYLID'],
                    'Multiplier': multiplier,
                    'Priority': form_data['Priority']}
                supabase_success = False
                if supabase:
                    with st.spinner("Inserting data into Supabase..."):
                        try:
                            response = supabase.table('live_dispatches').insert([data_to_insert]).execute()
                            st.success("Data successfully logged to `live_dispatches`!")
                            st.json(data_to_insert)
                            supabase_success = True
                        except Exception as e:
                            st.error(f"Failed to insert data into Supabase: {e}")
                if supabase_success:
                    with st.spinner("Sending resolution note to ConnectWise..."):
                        resolution_result = add_connectwise_resolution_note(
                            auth_headers, 
                            base_url, 
                            st.session_state.input_ticket_id, 
                            edited_note)
                    if resolution_result:
                        st.success(f"Resolution note successfully added to ticket {st.session_state.input_ticket_id}!")
                        st.session_state.ticket_form_data = None
                        st.session_state.input_ticket_id = ""
                        st.rerun()
                    else:
                        st.error("Failed to add resolution note to ticket.")
                else:
                    st.error("Skipping resolution note as Supabase insertion failed.")

# ------------------------------------------------- MAIN APP LOGIC -------------------------------------------------

PAGES = {
    "Landing Page": landing_page,
    "ConnectWise API": connectwise_page,
    "DXC Runbook": runbook_page,
    "Input Tickets": input_tickets_page,}
st.sidebar.title("Navigation")
page_selection = st.sidebar.radio("Go to", list(PAGES.keys()))
PAGES[page_selection]()