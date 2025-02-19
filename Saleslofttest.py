import requests
from datetime import datetime

# HubSpot API Key (Use OAuth for production)
HUBSPOT_API_KEY = "X"
SALESLOFT_API_KEY = "X"
HEADERS = {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}
SALESLOFT_HEADERS = {"Authorization": f"Bearer {SALESLOFT_API_KEY}", "Content-Type": "application/json"}

# Number of accounts to pull per run
ASSIGNMENT_LIMIT = 1

# Fetch companies from a list
def get_companies_from_list(list_id):
    url = f"https://api.hubapi.com/crm/v3/lists/{list_id}/companies"
    params = {"limit": ASSIGNMENT_LIMIT}
    response = requests.get(url, headers=HEADERS, params=params)
    return response.json().get("results", [])

# Fetch associated contacts for a company
def get_associated_contacts(company_id):
    url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}/associations/contacts"
    response = requests.get(url, headers=HEADERS)
    return response.json().get("results", [])

# Fetch SDRs from a HubSpot team
def get_sdrs_from_team(team_id):
    url = f"https://api.hubapi.com/crm/v3/owners"
    params = {"teamId": team_id, "limit": 100}
    response = requests.get(url, headers=HEADERS, params=params)
    owners = response.json().get("results", [])
    return {owner["email"]: {"id": owner["id"], "hubspot_id": owner["id"]} for owner in owners}

# Fetch SDR to Salesloft owner mapping
def get_salesloft_owner_id(sdr_email):
    url = "https://api.salesloft.com/v2/users.json"
    response = requests.get(url, headers=SALESLOFT_HEADERS)
    users = response.json().get("data", [])
    for user in users:
        if user.get("email") == sdr_email:
            return user.get("id")
    return None

# Push contacts into Salesloft
def push_contacts_to_salesloft(contacts, salesloft_owner_id):
    for contact in contacts:
        data = {
            "email": contact.get("email"),
            "first_name": contact.get("firstName"),
            "last_name": contact.get("lastName"),
            "owner_id": salesloft_owner_id
        }
        url = "https://api.salesloft.com/v2/people.json"
        requests.post(url, headers=SALESLOFT_HEADERS, json=data)
    print(f"Pushed {len(contacts)} contacts to Salesloft with owner_id {salesloft_owner_id}.")

# Rotate assignments using HubSpot's native rotation function
def rotate_assignments(companies, sdrs):
    sdr_emails = list(sdrs.keys())
    for i, company in enumerate(companies):
        assigned_sdr_email = sdr_emails[i % len(sdr_emails)]
        assigned_sdr = sdrs[assigned_sdr_email]
        assigned_sdr_id = assigned_sdr["id"]
        
        # Update the assigned SDR field in HubSpot
        url = f"https://api.hubapi.com/crm/v3/objects/companies/{company['id']}"
        data = {"properties": {"assigned_sdr": assigned_sdr_email, "assignment_date": datetime.utcnow().isoformat(), "hubspot_owner_id": assigned_sdr_id}}
        requests.patch(url, headers=HEADERS, json=data)
        
        contacts = get_associated_contacts(company['id'])
        salesloft_owner_id = get_salesloft_owner_id(assigned_sdr_email)
        
        if salesloft_owner_id:
            push_contacts_to_salesloft(contacts, salesloft_owner_id)
    
    print(f"Rotated {len(companies)} companies and pushed associated contacts to Salesloft.")

# Run assignment process
def main():
    list_id = "your_list_id"  # Replace with actual HubSpot list ID
    team_id = "your_team_id"  # Replace with actual HubSpot team ID
    companies = get_companies_from_list(list_id)
    sdrs = get_sdrs_from_team(team_id)
    rotate_assignments(companies, sdrs)

if __name__ == "__main__":
    main()
