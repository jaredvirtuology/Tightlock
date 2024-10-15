import requests
import json
from typing import Dict, Any, List, Optional
from enum import Enum
import hashlib
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PayloadType(Enum):
    CREATE_USER = "CREATE_USER"
    UPDATE_USER = "UPDATE_USER"

class MetaMarketingDestination:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.access_token = config.get("access_token")
        self.ad_account_id = config.get("ad_account_id")
        if not self.ad_account_id.startswith("act_"):
            self.ad_account_id = f"act_{self.ad_account_id}"
        self.payload_type = config.get("payload_type")
        self.api_version = "v17.0"
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        self.audience_name = config.get("audience_name", "Mon Audience CRM")

        self._validate_credentials()

    def _validate_credentials(self) -> None:
        if not self.access_token or not self.ad_account_id:
            raise ValueError("Token d'accès et ID du compte publicitaire requis.")
    def _check_token_validity(self) -> bool:
            url = f"{self.base_url}/debug_token"
            params = {
                "input_token": self.access_token,
                "access_token": self.access_token
            }
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json().get("data", {})
                if data.get("is_valid"):
                    logger.info("Token d'accès valide.")
                    return True
                else:
                    logger.error(f"Token d'accès invalide: {data.get('error', {}).get('message')}")
            else:
                logger.error(f"Erreur lors de la vérification du token: {response.text}")
            return False

    def _check_ad_account_access(self) -> bool:
        url = f"{self.base_url}/{self.ad_account_id}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            logger.info(f"Accès au compte publicitaire confirmé: {response.json().get('name')}")
            return True
        else:
            logger.error(f"Erreur d'accès au compte publicitaire: {response.text}")
            return False

    def _build_api_url(self) -> str:
        return f"{self.base_url}/{self.ad_account_id}/customaudiences"

    def _hash_data(self, data: str) -> str:
        return hashlib.sha256(data.encode('utf-8')).hexdigest()
    
    def _format_payload(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "name": self.audience_name,
            "subtype": "CUSTOM",
            "description": "Audience créée à partir des données CRM",
            "customer_file_source": "USER_PROVIDED_ONLY",
            "schema": ["EMAIL", "PHONE", "FN"],
            "data": [
                [
                    self._hash_data(item.get("email", "")),
                    self._hash_data(item.get("phone", "")),
                    item.get("first_name", "")
                ] for item in data
            ]
        }

    def _send_payload(self, payload: Dict[str, Any]) -> requests.Response:
        url = self._build_api_url()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response

    def send_data(self, input_data: List[Dict[str, Any]], dry_run: bool = False) -> Optional[Dict[str, Any]]:
        if not self._check_token_validity():
            logger.error("Token d'accès invalide. Veuillez générer un nouveau token.")
            return None

        if not self._check_ad_account_access():
            logger.error("Impossible d'accéder au compte publicitaire. Vérifiez l'ID du compte et les permissions.")
            return None

        try:
            payload = self._format_payload(input_data)
            if not dry_run:
                response = self._send_payload(payload)
                logger.info(f"Audience personnalisée créée/mise à jour avec succès: {response.json()}")
                return response.json()
            else:
                logger.info(f"Dry-run: Données qui seraient envoyées: {payload}")
                return payload
        except requests.HTTPError as e:
            logger.error(f"Erreur HTTP lors de la création/mise à jour de l'audience: {e}")
            logger.error(f"Réponse de l'API: {e.response.text}")
        except requests.RequestException as e:
            logger.error(f"Erreur lors de la création/mise à jour de l'audience: {str(e)}")
        return None
if __name__ == "__main__":
    config = {
        "access_token": "EAAL9plm0REcBO2xe5MrEKHbUGFxe28ZAFdpZBcjXkZBCWj1BBR5eOzucjtnSMuKcnimlNNZAKcyCJSEOBrYTbkD11eZAZAviqJBUG1DuOWXPQbmO0ZBBKY7OW6LzmS4XjlzvTCyMwr6ybZBRHwQZAG9DkPRrrX0JZBc2g8pGTZB3yTbmuCiAXwJhNqZBJwnFiBmbYKCxetWFaR2EZCeiFvXpEnqkZAhZBwky1Tp",
        "ad_account_id": "1158906525188725",
        "payload_type": PayloadType.CREATE_USER
    }

    destination = MetaMarketingDestination(config)

    test_data = [
        {"email": "test1@example.com", "phone": "1234567890", "first_name": "John"},
        {"email": "test2@example.com", "phone": "0987654321", "first_name": "Jane"}
    ]

    result = destination.send_data(test_data, dry_run=False)
