import pandas as pd
import os

# What I'm doing here is creating the logic for a matching payment using Lists in HubSpot
# From there, I'm exporting it, creating a dataframe for it plus the Held Commissions CSV
# We're joining "Email" from the HubsData CSV with "customer_external_key" from the Held Commissions CSV
# If there's a match, we'll update the "Pay" column in the Held Commissions CSV to "Pay"
# This is the list https://app.hubspot.com/contacts/20341965/objectLists/29498/filters
# It can be re-used with other partners if needed.

# Define desktop path
desktop_path = os.path.expanduser("~/Desktop")  # This works on both macOS and Windows

# File paths — ReName this in future if needed.
file1 = os.path.join(desktop_path, 'hubspot-crm-exports-held-comissions-2025-06-19-1.csv')
file2 = os.path.join(desktop_path, 'onholdcom.csv')
output_file = os.path.join(desktop_path, 'updated_payments_61925.csv')

df1 = pd.read_csv(file1)  # HubsData CSV
df2 = pd.read_csv(file2)  # Held Commissions CSV

# Defaults to don't pay. 
df2['Pay'] = "Don't Pay"

# This is where we do ze matching, ja? 
# If zere is ze match, we pay, ja? 
df2['Pay'] = df2['customer_external_key'].apply(
    lambda x: 'Pay' if x in df1['Email'].values else "Don't Pay"
)

# New CSV in the same order as your second for easy copying.
df2.to_csv(output_file, index=False)

# You can change this too. It's just a print and file name. 
print("Matching and updating complete. Updated file saved as 'updated_payments_2_61925.csv' on your Desktop.")
