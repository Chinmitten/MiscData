#!/usr/bin/env python3
"""
HubSpot to Avoma Transcript Pipeline

This script:
1. Retrieves contacts from a HubSpot list
2. Queries Avoma for meetings with those contacts
3. Retrieves call transcripts from Avoma
4. Sends transcripts to a webhook

Requirements:
- HubSpot Private App Token
- Avoma API Key
- Webhook URL
- Python 3.7+
- requests library (pip install requests)
"""

import requests
import json
import time
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hubspot_avoma_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class HubSpotAvomaPipeline:
    def __init__(
        self,
        hubspot_token: str,
        avoma_api_key: str,
        webhook_url: str,
        hubspot_list_id: str,
        rate_limit_delay: float = 0.5
    ):
        """
        Initialize the pipeline
        
        Args:
            hubspot_token: HubSpot private app token
            avoma_api_key: Avoma API key
            webhook_url: Webhook URL to send transcripts
            hubspot_list_id: HubSpot list ID to retrieve contacts from
            rate_limit_delay: Delay between API calls (seconds)
        """
        self.hubspot_token = hubspot_token
        self.avoma_api_key = avoma_api_key
        self.webhook_url = webhook_url
        self.hubspot_list_id = hubspot_list_id
        self.rate_limit_delay = rate_limit_delay
        
        # API endpoints
        self.hubspot_base_url = "https://api.hubapi.com"
        self.avoma_base_url = "https://api.avoma.com"
        
        # Headers
        self.hubspot_headers = {
            "Authorization": f"Bearer {hubspot_token}",
            "Content-Type": "application/json"
        }
        
        self.avoma_headers = {
            "Authorization": f"Bearer {avoma_api_key}",
            "Content-Type": "application/json"
        }
        
        self.webhook_headers = {
            "Content-Type": "application/json"
        }

    def get_hubspot_contacts(self) -> List[Dict[str, Any]]:
        """
        Retrieve contacts from HubSpot list using CRM API v3
        
        Returns:
            List of contact dictionaries with email addresses
        """
        logger.info(f"Retrieving contacts from HubSpot list: {self.hubspot_list_id}")
        
        try:
            # First, get the list details to understand its structure
            list_url = f"{self.hubspot_base_url}/crm/v3/lists/{self.hubspot_list_id}"
            list_response = requests.get(list_url, headers=self.hubspot_headers)
            
            if list_response.status_code != 200:
                logger.error(f"Failed to get list details: {list_response.status_code} - {list_response.text}")
                return []
            
            # Get contacts from the list
            contacts_url = f"{self.hubspot_base_url}/crm/v3/lists/{self.hubspot_list_id}/contacts"
            all_contacts = []
            after = None
            
            while True:
                params = {"limit": 100}
                if after:
                    params["after"] = after
                
                response = requests.get(contacts_url, headers=self.hubspot_headers, params=params)
                
                if response.status_code != 200:
                    logger.error(f"Failed to get contacts: {response.status_code} - {response.text}")
                    break
                
                data = response.json()
                contacts = data.get("results", [])
                all_contacts.extend(contacts)
                
                # Check if there are more pages
                paging = data.get("paging", {})
                after = paging.get("next", {}).get("after")
                
                if not after:
                    break
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
            
            logger.info(f"Retrieved {len(all_contacts)} contacts from HubSpot")
            return all_contacts
            
        except Exception as e:
            logger.error(f"Error retrieving HubSpot contacts: {str(e)}")
            return []

    def extract_emails_from_contacts(self, contacts: List[Dict[str, Any]]) -> List[str]:
        """
        Extract email addresses from HubSpot contacts
        
        Args:
            contacts: List of contact dictionaries from HubSpot
            
        Returns:
            List of email addresses
        """
        emails = []
        
        for contact in contacts:
            # Get contact properties
            contact_id = contact.get("id")
            if not contact_id:
                continue
            
            # Get detailed contact information
            contact_url = f"{self.hubspot_base_url}/crm/v3/objects/contacts/{contact_id}"
            contact_params = {"properties": "email,firstname,lastname"}
            
            try:
                response = requests.get(contact_url, headers=self.hubspot_headers, params=contact_params)
                
                if response.status_code == 200:
                    contact_data = response.json()
                    properties = contact_data.get("properties", {})
                    email = properties.get("email")
                    
                    if email:
                        emails.append(email)
                        logger.debug(f"Found email: {email}")
                
                time.sleep(self.rate_limit_delay)
                
            except Exception as e:
                logger.error(f"Error getting contact details for {contact_id}: {str(e)}")
                continue
        
        logger.info(f"Extracted {len(emails)} email addresses")
        return emails

    def get_avoma_meetings_for_email(self, email: str) -> List[Dict[str, Any]]:
        """
        Get Avoma meetings for a specific email address
        
        Args:
            email: Email address to search for
            
        Returns:
            List of meeting dictionaries
        """
        logger.debug(f"Searching Avoma meetings for: {email}")
        
        try:
            # Search for meetings by participant email
            meetings_url = f"{self.avoma_base_url}/v1/meetings"
            params = {
                "participantEmail": email,
                "limit": 100
            }
            
            response = requests.get(meetings_url, headers=self.avoma_headers, params=params)
            
            if response.status_code != 200:
                logger.error(f"Failed to get Avoma meetings for {email}: {response.status_code} - {response.text}")
                return []
            
            meetings = response.json().get("meetings", [])
            logger.debug(f"Found {len(meetings)} meetings for {email}")
            
            return meetings
            
        except Exception as e:
            logger.error(f"Error getting Avoma meetings for {email}: {str(e)}")
            return []

    def get_avoma_transcript(self, meeting_id: str) -> Optional[Dict[str, Any]]:
        """
        Get transcript for a specific Avoma meeting
        
        Args:
            meeting_id: Avoma meeting ID
            
        Returns:
            Transcript data or None if not found
        """
        logger.debug(f"Getting transcript for meeting: {meeting_id}")
        
        try:
            # Get meeting details including transcript
            meeting_url = f"{self.avoma_base_url}/v1/meetings/{meeting_id}"
            response = requests.get(meeting_url, headers=self.avoma_headers)
            
            if response.status_code != 200:
                logger.error(f"Failed to get meeting {meeting_id}: {response.status_code} - {response.text}")
                return None
            
            meeting_data = response.json()
            
            # Get transcript if available
            transcript_url = f"{self.avoma_base_url}/v1/meetings/{meeting_id}/transcript"
            transcript_response = requests.get(transcript_url, headers=self.avoma_headers)
            
            transcript_data = None
            if transcript_response.status_code == 200:
                transcript_data = transcript_response.json()
            else:
                logger.warning(f"No transcript available for meeting {meeting_id}")
            
            return {
                "meeting": meeting_data,
                "transcript": transcript_data
            }
            
        except Exception as e:
            logger.error(f"Error getting transcript for meeting {meeting_id}: {str(e)}")
            return None

    def send_to_webhook(self, data: Dict[str, Any]) -> bool:
        """
        Send data to webhook
        
        Args:
            data: Data to send to webhook
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = requests.post(
                self.webhook_url,
                json=data,
                headers=self.webhook_headers,
                timeout=30
            )
            
            if response.status_code in [200, 201, 202]:
                logger.info(f"Successfully sent data to webhook: {response.status_code}")
                return True
            else:
                logger.error(f"Webhook failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending to webhook: {str(e)}")
            return False

    def process_pipeline(self, date_filter_days: int = 30) -> Dict[str, Any]:
        """
        Run the complete pipeline
        
        Args:
            date_filter_days: Only process meetings from the last N days
            
        Returns:
            Summary of processing results
        """
        logger.info("Starting HubSpot to Avoma transcript pipeline")
        
        results = {
            "contacts_processed": 0,
            "emails_found": 0,
            "meetings_found": 0,
            "transcripts_retrieved": 0,
            "webhooks_sent": 0,
            "errors": []
        }
        
        try:
            # Step 1: Get contacts from HubSpot
            contacts = self.get_hubspot_contacts()
            results["contacts_processed"] = len(contacts)
            
            if not contacts:
                logger.warning("No contacts found in HubSpot list")
                return results
            
            # Step 2: Extract emails
            emails = self.extract_emails_from_contacts(contacts)
            results["emails_found"] = len(emails)
            
            if not emails:
                logger.warning("No email addresses found in contacts")
                return results
            
            # Step 3: Process each email
            for email in emails:
                logger.info(f"Processing email: {email}")
                
                # Get meetings for this email
                meetings = self.get_avoma_meetings_for_email(email)
                results["meetings_found"] += len(meetings)
                
                if not meetings:
                    logger.debug(f"No meetings found for {email}")
                    continue
                
                # Process each meeting
                for meeting in meetings:
                    meeting_id = meeting.get("id")
                    if not meeting_id:
                        continue
                    
                    # Check if meeting is within date filter
                    meeting_date = meeting.get("startTime")
                    if meeting_date:
                        meeting_datetime = datetime.fromisoformat(meeting_date.replace('Z', '+00:00'))
                        cutoff_date = datetime.now() - timedelta(days=date_filter_days)
                        if meeting_datetime < cutoff_date:
                            logger.debug(f"Skipping old meeting {meeting_id}")
                            continue
                    
                    # Get transcript
                    transcript_data = self.get_avoma_transcript(meeting_id)
                    if transcript_data:
                        results["transcripts_retrieved"] += 1
                        
                        # Prepare webhook payload
                        webhook_payload = {
                            "email": email,
                            "meeting_id": meeting_id,
                            "meeting_data": transcript_data["meeting"],
                            "transcript": transcript_data["transcript"],
                            "processed_at": datetime.now().isoformat(),
                            "source": "hubspot_avoma_pipeline"
                        }
                        
                        # Send to webhook
                        if self.send_to_webhook(webhook_payload):
                            results["webhooks_sent"] += 1
                        else:
                            results["errors"].append(f"Failed to send webhook for meeting {meeting_id}")
                    
                    # Rate limiting
                    time.sleep(self.rate_limit_delay)
            
            logger.info("Pipeline completed successfully")
            logger.info(f"Results: {json.dumps(results, indent=2)}")
            
        except Exception as e:
            error_msg = f"Pipeline failed: {str(e)}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
        
        return results

def main():
    """
    Main function - configure and run the pipeline
    """
    # Configuration - Set these values
    HUBSPOT_TOKEN = ""
    AVOMA_API_KEY = ""
    WEBHOOK_URL = ""
    HUBSPOT_LIST_ID = ""
    
    # Validate configuration
    if any(val == "your_" + key.lower() for key, val in [
        ("HUBSPOT_TOKEN", HUBSPOT_TOKEN),
        ("AVOMA_API_KEY", AVOMA_API_KEY),
        ("WEBHOOK_URL", WEBHOOK_URL),
        ("HUBSPOT_LIST_ID", HUBSPOT_LIST_ID)
    ]):
        logger.error("Please configure your API keys and URLs in the script")
        return
    
    # Initialize pipeline
    pipeline = HubSpotAvomaPipeline(
        hubspot_token=HUBSPOT_TOKEN,
        avoma_api_key=AVOMA_API_KEY,
        webhook_url=WEBHOOK_URL,
        hubspot_list_id=HUBSPOT_LIST_ID,
        rate_limit_delay=0.5  # 500ms between API calls
    )
    
    # Run pipeline
    results = pipeline.process_pipeline(date_filter_days=30)  # Last 30 days
    
    # Print summary
    print("\n" + "="*50)
    print("PIPELINE SUMMARY")
    print("="*50)
    print(f"Contacts processed: {results['contacts_processed']}")
    print(f"Emails found: {results['emails_found']}")
    print(f"Meetings found: {results['meetings_found']}")
    print(f"Transcripts retrieved: {results['transcripts_retrieved']}")
    print(f"Webhooks sent: {results['webhooks_sent']}")
    
    if results['errors']:
        print(f"Errors: {len(results['errors'])}")
        for error in results['errors']:
            print(f"  - {error}")
    
    print("="*50)

if __name__ == "__main__":
    main()
