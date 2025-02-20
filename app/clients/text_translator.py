from azure.ai.translation.text import TextTranslationClient, TranslatorCredential
from azure.ai.translation.text.models import InputTextItem
from azure.core.exceptions import HttpResponseError

class TextTranslator:
    def __init__(self, key: str, endpoint: str, region: str, category_id: str = None) -> None:
        credential = TranslatorCredential(key, region)
        self.text_translator = TextTranslationClient(endpoint=endpoint, credential=credential)
        self.category_id = category_id
    
    def set_category_id(self, category_id: str) -> None:
        self.category_id = category_id

    def translate(self, target_lang: str, sentence: InputTextItem) -> str:
        to_language = [target_lang]

        try:
            response = self.text_translator.translate(content=[sentence], to=to_language, category=self.category_id)
            res = response[0].translations[0].text
            return res

        except HttpResponseError as exception:
            if exception.error is not None:
                print(f"Error Code: {exception.error.code}")
                print(f"Message: {exception.error.message}")