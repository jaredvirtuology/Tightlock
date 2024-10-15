import requests
import json
import os
from enum import Enum

# Use environment variables for configuration
TIGHTLOCK_IP = os.getenv('TIGHTLOCK_IP', '{ADDRESS}')
API_KEY = os.getenv('TIGHTLOCK_API_KEY', '{EXAMPLE_API_KEY}')

BASE_URL = f"http://{TIGHTLOCK_IP}/api/v1"

headers = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY
}

class PayloadType(Enum):
    CREATE_USER = "CREATE_USER"
    UPDATE_USER = "UPDATE_USER"

def create_new_config(config_data):
    """
    Create a new configuration in Tightlock.
    
    :param config_data: dict containing the configuration data
    :return: Response from the API
    """
    url = f"{BASE_URL}/configs"
    response = requests.post(url, headers=headers, json=config_data)
    return response.json()

def get_current_config():
    """
    Get the current configuration from Tightlock.
    
    :return: Current configuration in JSON format
    """
    url = f"{BASE_URL}/configs:getLatest"
    response = requests.get(url, headers=headers)
    return response.json()

def trigger_connection(connection_name, dry_run=False):
    """
    Trigger an existing connection in Tightlock.
    
    :param connection_name: Name of the connection to trigger
    :param dry_run: Boolean indicating whether to perform a dry run
    :return: Response from the API
    """
    url = f"{BASE_URL}/activations/{connection_name}:trigger"
    data = {"dry_run": 1 if dry_run else 0}
    response = requests.post(url, headers=headers, json=data)
    return response.json()

def test_connection():
    """
    Test the connection to the Tightlock API.
    
    :return: Response from the API
    """
    url = f"{BASE_URL}/connect"
    response = requests.post(url, headers=headers)
    return response.text, response.status_code

# Example usage:
if __name__ == "__main__":
    # Test the connection
    response, status_code = test_connection()
    print(f"Connection test response (status {status_code}): {response}")
    
    # Example configuration with Meta Marketing destination
    new_config = {
        "label": "Example BQ to Meta Marketing",
        "value": {
            "external_connections": [], 

            "sources": {
                "example_bigquery_table": {
                    "type": "BIGQUERY",
                    "dataset": "bq_dataset_example_name",
                    "table": "bq_table_example_name"
                }
            },

            "destinations": {
                "example_meta_marketing": {
                    "type": "META_MARKETING",
                    "access_token": "YOUR_ACCESS_TOKEN_HERE",
                    "ad_account_id": "YOUR_AD_ACCOUNT_ID_HERE",
                    "payload_type": PayloadType.CREATE_USER.value,
                    "audience_name": "Tightlock Test Audience"
                }   
            },

            "activations": [
                {
                    "name": "example_bq_to_meta_marketing",
                    "source": {
                        "$ref": "#/sources/example_bigquery_table"
                    },
                    "destination": {
                        "$ref": "#/destinations/example_meta_marketing"
                    },
                    "schedule": "@weekly"
                }
            ], 

            "secrets": {},
        }
    }
    
    # Create new config
    print("Creating new config:")
    print(create_new_config(new_config))

    # Get current config
    print("\nGetting current config:")
    print(get_current_config())

    # Trigger the connection (dry run)
    print("\nTriggering connection (dry run):")
    print(trigger_connection("example_bq_to_meta_marketing", dry_run=True))

    # Trigger the connection (actual run)
    print("\nTriggering connection (actual run):")
    print(trigger_connection("example_bq_to_meta_marketing", dry_run=False))