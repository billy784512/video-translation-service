
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    class Translator:
        ENDPOINT = os.getenv("TRANSLATOR_ENDPOINT")
        REGION = os.getenv("TRANSLATOR_REGION")
        KEY = os.getenv("TRANSLATOR_KEY")

    class VideoTranslation:
        KEY = os.getenv("TRANSLATION_API_KEY")
        BASE_URL = os.getenv("TRANSLATION_API_URL")
    
    class EventHub:
        CONN_STR = os.getenv("EVENT_HUB_CONNECTION_STRING")

    class BlobStorage:
        CONN_STR = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
        SOURCE_CONTAINER = os.getenv("BLOB_STORAGE_SOURCE_CONTAINER")
        TARGET_CONTAINER = os.getenv("BLOB_STORAGE_TARGET_CONTAINER")

config = Config()