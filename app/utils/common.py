import requests

from azure.eventhub import EventHubProducerClient

from .config import config

def get_event_hub_producer(event_hub_name: str) -> EventHubProducerClient:
    if not config.EventHub.CONN_STR:
        raise ValueError("EVENT_HUB_CONNECTION_STRING is not set")
    return EventHubProducerClient.from_connection_string(config.EventHub.CONN_STR, eventhub_name=event_hub_name)

def fetch_operation_status(operation_id: str) -> dict:
    url = f"{config.VideoTranslation.BASE_URL}/operations/{operation_id}?api-version=2024-05-20-preview"
    headers = {"Ocp-Apim-Subscription-Key": config.VideoTranslation.KEY}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

