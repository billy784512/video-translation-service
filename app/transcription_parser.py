import json
import uuid
import logging

class TranscriptionParser:
   def __init__(self):
      # TODO: 改寫成 dict, with zh-CN and en-US voice
      self.voice_templates = ["en-US-AndrewNeural", "en-US-GuyNeural", "en-US-AIGenerate1Neural", "en-US-BrandonNeural", "en-US-BrianNeural", "en-US-ChristopherNeural"]
      self.ptr = 0

   def check_for_reset_ptr(self) -> None:
      if self.ptr >= len(self.voice_templates):
         self.ptr = 0

   def parse_and_save(self, input_json_path: str, output_path: str) -> None:
      try:
         raw_transcription = ""
         with open(input_json_path, "r", encoding="utf-8") as f:
            raw_transcription = json.load(f)
         
         parse_result = self.parse(raw_transcription)

         with open(output_path, "w", encoding="utf-8") as f:
            f.write(parse_result)
      except Exception as e:
            logging.info(f"Failed to parse json: {e}")
            raise

   def parse(self, raw_transcription: dict) -> str:
      try:
         phrases = raw_transcription.get("phrases", [])
         output = "WEBVTT\n\n"
         vtt_entries = ["" for _ in range(len(phrases))]
         speakers = {}

         idx = len(phrases)-1
         for phrase in phrases[::-1]:
            start_time_ms = phrase.get("offsetMilliseconds", 0)
            duration_ms = phrase.get("durationMilliseconds", 0)
            end_time_ms = start_time_ms + duration_ms

            # Convert milliseconds to VTT timestamp format
            start_time = self.__ms_to_vtt_timestamp(start_time_ms)
            end_time = self.__ms_to_vtt_timestamp(end_time_ms)

            speaker_id = f"Speaker{phrase.get('speaker', 'Unknown')}"
            text = phrase.get("text", "")

            if speaker_id not in speakers:
               speakers[speaker_id] = {
                  "defaultSsmlProperties": {
                     "voiceName": self.voice_templates[self.ptr],
                     "voiceKind": "PlatformVoice"
                  }
               }

            self.ptr += 1
            self.check_for_reset_ptr()

            if idx > 0:
               vtt_entry = f"{start_time} --> {end_time}\n{{\n  \"id\": \"{self.__generate_uuid()}\",\n  \"gender\": \"Male\",\n  \"speakerId\": \"{speaker_id}\",\n  \"ssmlProperties\": {{}},\n  \"sourceLocaleText\": \"{text}\",\n  \"translatedText\": \"\"\n}}"
            else:
               global_metadata = {
                  "globalMetadata": {
                     "speakers": speakers
                  }
               }
               global_metadata_str = json.dumps(global_metadata, indent=2)[1:-1].rstrip() + ",\n"
               vtt_entry = f"{start_time} --> {end_time}\n{{{global_metadata_str}  \"id\": \"{self.__generate_uuid()}\",\n  \"gender\": \"Male\",\n  \"speakerId\": \"{speaker_id}\",\n  \"ssmlProperties\": {{}},\n  \"sourceLocaleText\": \"{text}\",\n  \"translatedText\": \"\"\n}}"

            vtt_entries[idx] = (vtt_entry)
            idx -= 1
         
         output += "\n\n".join(vtt_entries)   
         return output
      except Exception as e:
         logging.info(f"Failed to parse json: {e}")
         raise

   def __ms_to_vtt_timestamp(self, ms: int) -> str:
      hours = ms // 3600000
      minutes = (ms % 3600000) // 60000
      seconds = (ms % 60000) // 1000
      milliseconds = ms % 1000
      return f"{hours:02}:{minutes:02}:{seconds:02}.{milliseconds:03}"

   def __generate_uuid(self) -> str:
      return str(uuid.uuid4())
