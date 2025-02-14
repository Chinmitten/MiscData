#searches for HubSPot companies based on a rough name match, exports to a csv for review.

import requests
import pandas as pd

API_KEY = ''
SEARCH_URL = 'https://api.hubapi.com/crm/v3/objects/companies/search'

headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

def search_companies(company_names):
    results = []
    
    for name in company_names:
        data = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "name",
                            "operator": "CONTAINS_TOKEN",
                            "value": name
                        }
                    ]
                }
            ],
            "properties": [
                "name", 
                "hs_object_id", 
                "hs_num_associated_deals", 
                "hs_last_activity_date", 
                "mrr", 
                "createdate"
            ]
        }
        
        response = requests.post(SEARCH_URL, headers=headers, json=data)
        
        if response.status_code == 200:
            companies = response.json().get('results', [])
            if companies:
                for company in companies:
                    last_activity = company['properties'].get('hs_last_activity_date', 'N/A')
                    create_date = company['properties'].get('createdate', 'N/A')
                    
                    if last_activity != 'N/A':
                        last_activity = pd.to_datetime(last_activity).strftime('%Y-%m-%d')
                    if create_date != 'N/A':
                        create_date = pd.to_datetime(create_date).strftime('%Y-%m-%d')
                    
                    results.append({
                        'Searched Name': name,
                        'Company Name': company['properties'].get('name', 'N/A'),
                        'Record ID': company['properties'].get('hs_object_id', 'N/A'),
                        'Associated Deals': company['properties'].get('hs_num_associated_deals', 'N/A'),
                        'Last Activity Date': last_activity,
                        'MRR': company['properties'].get('mrr', 'N/A'),
                        'Create Date': create_date,
                        'Match Found': 'Yes'
                    })
            else:
                results.append({
                    'Searched Name': name,
                    'Company Name': 'No Match Found',
                    'Record ID': 'N/A',
                    'Associated Deals': 'N/A',
                    'Last Activity Date': 'N/A',
                    'MRR': 'N/A',
                    'Create Date': 'N/A',
                    'Match Found': 'No'
                })
        else:
            print(f"Failed to search for company: {name}. Status code: {response.status_code}")
    
    return results

def save_to_csv(data, filename='AidanReport.csv'):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

if __name__ == "__main__":
    company_names = [
        "company name go here lmao"
    ]
    
    search_results = search_companies(company_names)
    save_to_csv(search_results)
