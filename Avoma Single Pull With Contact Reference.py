#python3 avoma_transcript_extractor.py --meeting-id 278f0224-2859-47f6-8d54-01865f120115 --api-key xxxx --webhook-url https://api.clay.com/v3/sources/webhook/pull-in-data-from-a-webhook-c823042a-9656-4459-85ab-b16b182635eb --reference-email mat.guthrie@dharmabums.com.au --verbose#!/usr/bin/env python3
#examplerun

"""
Avoma Meeting Transcript Extractor with Webhook Support

This script extracts plain text transcripts from Avoma meetings using a meeting ID
and optionally sends the transcript to a webhook with reference email.

Usage:
    python avoma_transcript_extractor.py --meeting-id <meeting_id> --api-key <api_key>
    python avoma_transcript_extractor.py --meeting-id <meeting_id> --api-key <api_key> --webhook-url <webhook_url> --reference-email <email>
    python avoma_transcript_extractor.py --meeting-id <meeting_id> --api-key <api_key> --output transcript.txt --webhook-url <webhook_url> --reference-email <email>

Requirements:
    - Avoma API key
    - requests library
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from typing import Dict, Optional, Any

import requests


class AvomaTranscriptExtractor:
    """Extract transcripts from Avoma meetings and send to webhook."""
    
    def __init__(self, api_key: str, base_url: str = "https://api.avoma.com/v1"):
        """
        Initialize the Avoma transcript extractor.
        
        Args:
            api_key: Avoma API key for authentication
            base_url: Base URL for Avoma API
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'User-Agent': 'Avoma-Transcript-Extractor/1.0'
        })
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def get_meeting_data(self, meeting_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve meeting metadata from Avoma API.
        
        Args:
            meeting_id: Avoma meeting ID
            
        Returns:
            Meeting metadata or None if failed
        """
        try:
            url = f"{self.base_url}/meetings/{meeting_id}"
            self.logger.info(f"Fetching meeting data from: {url}")
            
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                self.logger.info("Successfully retrieved meeting metadata")
                return data
            else:
                self.logger.error(f"API request failed with status {response.status_code}")
                if response.status_code == 401:
                    self.logger.error("Authentication failed. Please check your API key.")
                elif response.status_code == 403:
                    self.logger.error("Access forbidden. Please check your API permissions.")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error while fetching meeting data: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing JSON response: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error while fetching meeting data: {e}")
            return None
    
    def get_transcript_data(self, transcription_uuid: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve transcript data using transcription UUID.
        
        Args:
            transcription_uuid: Transcription UUID from meeting data
            
        Returns:
            Transcript data or None if failed
        """
        try:
            # Try different possible endpoints for transcript
            endpoints_to_try = [
                f"{self.base_url}/transcriptions/{transcription_uuid}",
                f"{self.base_url}/transcriptions/{transcription_uuid}/transcript",
                f"{self.base_url}/transcripts/{transcription_uuid}",
                f"{self.base_url}/transcripts/{transcription_uuid}/content"
            ]
            
            for endpoint in endpoints_to_try:
                self.logger.info(f"Trying transcript endpoint: {endpoint}")
                
                response = self.session.get(endpoint, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    self.logger.info(f"Successfully retrieved transcript from {endpoint}")
                    return data
                elif response.status_code == 404:
                    self.logger.warning(f"Transcript not found at {endpoint}")
                    continue
                else:
                    self.logger.warning(f"Transcript request failed with status {response.status_code} at {endpoint}")
            
            return None
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error while fetching transcript: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing transcript JSON response: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error while fetching transcript: {e}")
            return None
    
    def extract_plain_text_transcript(self, transcript_data: Dict[str, Any]) -> str:
        """
        Extract plain text transcript from transcript data.
        
        Args:
            transcript_data: Raw transcript data from Avoma API
            
        Returns:
            Plain text transcript
        """
        transcript_text = ""
        
        # Try different possible keys for transcript data
        transcript_keys = ['transcript', 'transcript_text', 'text', 'content', 'meeting_transcript', 'segments']
        
        for key in transcript_keys:
            if key in transcript_data and transcript_data[key]:
                if isinstance(transcript_data[key], str):
                    transcript_text = transcript_data[key]
                    break
                elif isinstance(transcript_data[key], list):
                    # Handle case where transcript is a list of segments
                    transcript_segments = []
                    for segment in transcript_data[key]:
                        if isinstance(segment, dict):
                            # Extract text from segment
                            segment_text = segment.get('text', segment.get('content', segment.get('transcript', segment.get('speaker_text', ''))))
                            if segment_text:
                                # Optionally include speaker info
                                speaker = segment.get('speaker', segment.get('speaker_name', ''))
                                if speaker:
                                    transcript_segments.append(f"{speaker}: {segment_text}")
                                else:
                                    transcript_segments.append(segment_text)
                        elif isinstance(segment, str):
                            transcript_segments.append(segment)
                    transcript_text = '\n'.join(transcript_segments)
                    break
        
        # If no transcript found in main data, check nested structures
        if not transcript_text:
            # Check for nested transcript data
            if 'data' in transcript_data:
                transcript_text = self.extract_plain_text_transcript(transcript_data['data'])
            elif 'transcript' in transcript_data:
                transcript_text = self.extract_plain_text_transcript(transcript_data['transcript'])
        
        return transcript_text.strip()
    
    def send_to_webhook(self, transcript: str, meeting_id: str, webhook_url: str, reference_email: Optional[str] = None, additional_data: Optional[Dict] = None) -> bool:
        """
        Send transcript to webhook.
        
        Args:
            transcript: Plain text transcript
            meeting_id: Meeting ID for reference
            webhook_url: Webhook URL to send data to
            reference_email: Optional reference email to include in payload
            additional_data: Additional data to include in webhook payload
            
        Returns:
            True if successful, False otherwise
        """
        try:
            payload = {
                'meeting_id': meeting_id,
                'transcript': transcript,
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'source': 'avoma',
                'transcript_length': len(transcript)
            }
            
            # Add reference email if provided
            if reference_email:
                payload['reference_email'] = reference_email
            
            if additional_data:
                payload.update(additional_data)
            
            self.logger.info(f"Sending transcript to webhook: {webhook_url}")
            if reference_email:
                self.logger.info(f"Reference email: {reference_email}")
            
            response = requests.post(
                webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if response.status_code in [200, 201, 202]:
                self.logger.info("Transcript successfully sent to webhook")
                return True
            else:
                self.logger.error(f"Webhook request failed with status {response.status_code}")
                self.logger.error(f"Response: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error while sending to webhook: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error while sending to webhook: {e}")
            return False
    
    def get_transcript(self, meeting_id: str) -> Optional[str]:
        """
        Get plain text transcript for a meeting.
        
        Args:
            meeting_id: Avoma meeting ID
            
        Returns:
            Plain text transcript or None if failed
        """
        self.logger.info(f"Processing meeting ID: {meeting_id}")
        
        # Get meeting metadata first
        meeting_data = self.get_meeting_data(meeting_id)
        if not meeting_data:
            self.logger.error("Failed to retrieve meeting metadata")
            return None
        
        # Check if transcript is ready
        if not meeting_data.get('transcript_ready', False):
            self.logger.error("Transcript is not ready for this meeting")
            return None
        
        # Get transcription UUID
        transcription_uuid = meeting_data.get('transcription_uuid')
        if not transcription_uuid:
            self.logger.error("No transcription UUID found in meeting data")
            return None
        
        self.logger.info(f"Found transcription UUID: {transcription_uuid}")
        
        # Get transcript data
        transcript_data = self.get_transcript_data(transcription_uuid)
        if not transcript_data:
            self.logger.error("Failed to retrieve transcript data")
            return None
        
        # Extract transcript
        transcript = self.extract_plain_text_transcript(transcript_data)
        if not transcript:
            self.logger.error("No transcript found in transcript data")
            return None
        
        self.logger.info(f"Extracted transcript ({len(transcript)} characters)")
        return transcript
    
    def process_meeting(self, meeting_id: str, webhook_url: Optional[str] = None, output_file: Optional[str] = None, reference_email: Optional[str] = None) -> bool:
        """
        Process a meeting to extract transcript and optionally send to webhook or save to file.
        
        Args:
            meeting_id: Avoma meeting ID
            webhook_url: Optional webhook URL to send transcript to
            output_file: Optional file path to save transcript to
            reference_email: Optional reference email to include in webhook payload
            
        Returns:
            True if successful, False otherwise
        """
        # Get transcript
        transcript = self.get_transcript(meeting_id)
        if not transcript:
            return False
        
        success = True
        
        # Send to webhook if provided
        if webhook_url:
            webhook_success = self.send_to_webhook(
                transcript, 
                meeting_id, 
                webhook_url,
                reference_email,
                {
                    'meeting_title': 'Avoma Meeting',  # You can extract this from meeting_data if needed
                    'transcript_format': 'plain_text'
                }
            )
            if not webhook_success:
                success = False
        
        # Save to file if provided
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(transcript)
                self.logger.info(f"Transcript saved to {output_file}")
            except Exception as e:
                self.logger.error(f"Error saving transcript to file: {e}")
                success = False
        
        return success


def main():
    """Main function to handle command line arguments and execute transcript extraction."""
    parser = argparse.ArgumentParser(
        description="Extract Avoma meeting transcripts and send to webhook with reference email",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract transcript and send to webhook with reference email
  python avoma_transcript_extractor.py --meeting-id abc123 --api-key your_api_key --webhook-url https://your-webhook.com/endpoint --reference-email user@example.com
  
  # Extract transcript, save to file, and send to webhook with reference email
  python avoma_transcript_extractor.py --meeting-id abc123 --api-key your_api_key --output transcript.txt --webhook-url https://your-webhook.com/endpoint --reference-email user@example.com
  
  # Extract transcript and save to file only (no webhook)
  python avoma_transcript_extractor.py --meeting-id abc123 --api-key your_api_key --output transcript.txt
        """
    )
    
    parser.add_argument('--meeting-id', required=True, help='Avoma meeting ID')
    parser.add_argument('--api-key', required=True, help='Avoma API key')
    parser.add_argument('--webhook-url', help='Webhook URL to send transcript to')
    parser.add_argument('--reference-email', help='Reference email to include in webhook payload')
    parser.add_argument('--output', '-o', help='Output file path (optional, prints to stdout if not specified)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set up logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate that at least one output method is specified
    if not args.webhook_url and not args.output:
        print("❌ Error: You must specify either --webhook-url or --output (or both)")
        sys.exit(1)
    
    # Create extractor and process meeting
    extractor = AvomaTranscriptExtractor(args.api_key)
    success = extractor.process_meeting(
        meeting_id=args.meeting_id,
        webhook_url=args.webhook_url,
        output_file=args.output,
        reference_email=args.reference_email
    )
    
    if success:
        print("✅ Transcript processing completed successfully")
        if args.webhook_url:
            print(f"�� Transcript sent to webhook: {args.webhook_url}")
            if args.reference_email:
                print(f"📧 Reference email included: {args.reference_email}")
        if args.output:
            print(f"�� Transcript saved to: {args.output}")
        sys.exit(0)
    else:
        print("❌ Transcript processing failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
