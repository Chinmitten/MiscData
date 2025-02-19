import requests

access_token = 'X'

url = 'https://api.hubapi.com/settings/v3/users/teams'

headers = {
    'Authorization': f'Bearer {access_token}',  # Use 'Bearer {access_token}' for OAuth token
}
response = requests.get(url, headers=headers)

if response.status_code == 200:
    teams = response.json()
    for team in teams['results']:
        print(f"Team Name: {team['name']}, Team ID: {team['id']}")
else:
    print(f"Error: {response.status_code} - {response.text}")

