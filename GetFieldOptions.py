import requests

HUBSPOT_API_URL = "https://api.hubapi.com"
PROPERTY_GROUP = "contacts"
PROPERTY_NAME = "" 
ACCESS_TOKEN = ""  

def get_property_options():
    url = f"{HUBSPOT_API_URL}/crm/v3/properties/{PROPERTY_GROUP}/{PROPERTY_NAME}"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        property_data = response.json()
        options = property_data.get("options", [])
        
        print(f"Available options for '{PROPERTY_NAME}':")
        for option in options:
            print(f"Label: {option['label']}, Value: {option['value']}")
    else:
        print(f"Failed to fetch property details. Status Code: {response.status_code}")
        print("Response:", response.json())

if __name__ == "__main__":
    get_property_options()
