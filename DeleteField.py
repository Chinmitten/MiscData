#Permanently deletes a HubSpot property from the selet object type. Swap type to whatever you need
#and use the property internal name. 
# I just run this locally via VsCode

import requests

ACCESS_TOKEN = '' #add access token
OBJECT_TYPE = 'companies'  
PROPERTY_NAME = 'churn_request_timestamp'

url = f'https://api.hubapi.com/crm/v3/properties/{OBJECT_TYPE}/{PROPERTY_NAME}'

headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

response = requests.delete(url, headers=headers)

if response.status_code == 204:
    print(f"Property '{PROPERTY_NAME}' successfully deleted from {OBJECT_TYPE}.")
elif response.status_code == 404:
    print(f"Property '{PROPERTY_NAME}' not found on {OBJECT_TYPE}.")
else:
    print(f"Failed to delete property. Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
