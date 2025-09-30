#!/usr/bin/env python3
"""
HubSpot Object Properties Fetcher and CSV Exporter

This script retrieves properties for a given HubSpot object type (contacts, companies, deals, etc.)
along with their field options and descriptions, then exports them to CSV format.

Usage:
    python hubspot_properties.py --object-type contacts --access-token YOUR_TOKEN
    python hubspot_properties.py --object-type companies --access-token YOUR_TOKEN --output-file companies_properties.csv
"""

import argparse
import csv
import json
import sys
from typing import Dict, List, Optional
import requests


class HubSpotPropertiesFetcher:
    """Fetches properties from HubSpot objects and exports them to CSV."""
    
    def __init__(self, access_token: str):
        """
        Initialize the fetcher with HubSpot access token.
        
        Args:
            access_token: HubSpot private app access token
        """
        self.access_token = access_token
        self.base_url = "https://api.hubapi.com"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    def get_object_properties(self, object_type: str) -> Dict:
        """
        Fetch all properties for a given HubSpot object type.
        
        Args:
            object_type: The HubSpot object type (e.g., 'contacts', 'companies', 'deals')
            
        Returns:
            Dictionary containing properties with their details
        """
        url = f"{self.base_url}/crm/v3/properties/{object_type}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching properties for {object_type}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response content: {e.response.text}")
            return {}
    
    def format_property_for_csv(self, property_data: Dict) -> Dict:
        """
        Format property data for CSV export.
        
        Args:
            property_data: Raw property data from HubSpot API
            
        Returns:
            Formatted property information for CSV
        """
        # Format options as a readable string
        options_str = ""
        if property_data.get("options"):
            options_list = []
            for option in property_data["options"]:
                option_text = f"{option.get('label', 'N/A')} ({option.get('value', 'N/A')})"
                if option.get('description'):
                    option_text += f" - {option['description']}"
                options_list.append(option_text)
            options_str = "; ".join(options_list)
        
        return {
            "name": property_data.get("name", ""),
            "label": property_data.get("label", ""),
            "description": property_data.get("description", ""),
            "type": property_data.get("type", ""),
            "field_type": property_data.get("fieldType", ""),
            "group_name": property_data.get("groupName", ""),
            "options": options_str,
            "calculated": property_data.get("calculated", False),
            "external_options": property_data.get("externalOptions", False),
            "has_unique_value": property_data.get("hasUniqueValue", False),
            "hidden": property_data.get("hidden", False),
            "hubspot_defined": property_data.get("hubspotDefined", False),
            "display_order": property_data.get("displayOrder", ""),
            "read_only_definition": property_data.get("readOnlyDefinition", False),
            "read_only_value": property_data.get("readOnlyValue", False),
            "searchable_in_global_search": property_data.get("searchableInGlobalSearch", False),
            "show_currency_symbol": property_data.get("showCurrencySymbol", False),
            "created_at": property_data.get("createdAt", ""),
            "updated_at": property_data.get("updatedAt", "")
        }
    
    def export_to_csv(self, properties: List[Dict], object_type: str, filename: Optional[str] = None):
        """
        Export properties to CSV file.
        
        Args:
            properties: List of property dictionaries
            object_type: The HubSpot object type
            filename: Optional custom filename
        """
        if not filename:
            filename = f"hubspot_{object_type}_properties.csv"
        
        # Define CSV headers
        headers = [
            "name", "label", "description", "type", "field_type", "group_name", 
            "options", "calculated", "external_options", "has_unique_value", 
            "hidden", "hubspot_defined", "display_order", "read_only_definition", 
            "read_only_value", "searchable_in_global_search", "show_currency_symbol", 
            "created_at", "updated_at"
        ]
        
        # Format properties for CSV
        formatted_properties = [self.format_property_for_csv(prop) for prop in properties]
        
        # Write to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(formatted_properties)
        
        print(f"\n✅ Properties exported to: {filename}")
        print(f"📊 Total properties exported: {len(formatted_properties)}")
    
    def print_summary(self, properties: List[Dict], object_type: str):
        """
        Print a summary of properties for the given object type.
        
        Args:
            properties: List of property dictionaries
            object_type: The HubSpot object type
        """
        print(f"\n{'='*60}")
        print(f"HubSpot {object_type.title()} Properties Summary")
        print(f"{'='*60}")
        print(f"Total properties found: {len(properties)}")
        
        # Group properties by type
        type_counts = {}
        for prop in properties:
            prop_type = prop.get("type", "unknown")
            type_counts[prop_type] = type_counts.get(prop_type, 0) + 1
        
        print(f"\nProperty types breakdown:")
        for prop_type, count in sorted(type_counts.items()):
            print(f"  {prop_type}: {count}")
        
        # Show properties with options
        properties_with_options = [p for p in properties if p.get("options")]
        if properties_with_options:
            print(f"\nProperties with field options: {len(properties_with_options)}")
            for prop in properties_with_options[:5]:  # Show first 5
                print(f"  - {prop.get('name')} ({len(prop.get('options', []))} options)")
            if len(properties_with_options) > 5:
                print(f"  ... and {len(properties_with_options) - 5} more")


def main():
    """Main function to handle command line arguments and execute the script."""
    parser = argparse.ArgumentParser(
        description="Fetch HubSpot object properties and export to CSV"
    )
    parser.add_argument(
        "--object-type",
        required=True,
        choices=["contacts", "companies", "deals", "tickets", "products", "line_items", 
                "quotes", "calls", "emails", "meetings", "notes", "tasks", 
                "communications", "feedback_submissions", "postal_mail", "conversations"],
        help="HubSpot object type to fetch properties for"
    )
    parser.add_argument(
        "--access-token",
        required=True,
        help="HubSpot private app access token"
    )
    parser.add_argument(
        "--output-file",
        help="Optional filename for CSV output (default: hubspot_{object_type}_properties.csv)"
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Show only summary information without CSV export"
    )
    
    args = parser.parse_args()
    
    # Initialize the fetcher
    fetcher = HubSpotPropertiesFetcher(args.access_token)
    
    # Fetch properties
    print(f"🔍 Fetching properties for {args.object_type}...")
    result = fetcher.get_object_properties(args.object_type)
    
    if not result or "results" not in result:
        print(f"❌ Failed to fetch properties for {args.object_type}")
        sys.exit(1)
    
    properties = result["results"]
    
    # Print summary
    fetcher.print_summary(properties, args.object_type)
    
    # Export to CSV unless summary-only is requested
    if not args.summary_only:
        fetcher.export_to_csv(properties, args.object_type, args.output_file)
    else:
        print("\n📋 Summary only mode - no CSV export performed")


if __name__ == "__main__":
    main()
