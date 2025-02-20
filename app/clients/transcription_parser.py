import re

from .text_translator import TextTranslator
from azure.ai.translation.text.models import InputTextItem


class TranscriptionParser:
   def __init__(self, lang: str = "zh-TW", translator: TextTranslator = None):
      self.translator = translator
      self.lang = lang

   def set_translator(self, translator: TextTranslator):
      self.translator = translator

   def easy_parse(self, json_path: str) -> None:
      with open(json_path, 'r', encoding='utf-8') as file:
         content = file.read()

      blocks = re.split(r'\n(?=\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3})', content)
      processed_blocks = []

      for block in blocks:
         match = re.search(r'"translatedText": "(.*?)"', block)
         match2 = re.search(r'"sourceLocaleText": "(.*?)"', block)
         if match and match2:
               original_text = match.group(1)
               original_text_2 = match2.group(1)
               translated_text = self.translator.translate(self.lang, InputTextItem(text=original_text_2))
               block = block.replace(f'"translatedText": "{original_text}"', f'"translatedText": "{translated_text}"')
         processed_blocks.append(block)

      processed_content = "\n".join(processed_blocks)

      with open(json_path, 'w', encoding='utf-8') as file:
         file.write(processed_content)
