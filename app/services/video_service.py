import os
import copy
import json
import logging
import uuid
import requests
import time
from typing import Dict

from azure.eventhub import EventData

from utils import ffmpeg, common, config
from utils.tmp_dir_manager import TmpDirManager
from clients.azure_blob_manager import AzureBlobManager
from clients.transcription_parser import TranscriptionParser
from clients.text_translator import TextTranslator


def split_video(body: Dict) -> int:

    blob_name = body.get("blob_name")
    chunk_size = body.get("chunk_size", 100)

    try:
        blob_manager = AzureBlobManager(config.BlobStorage.CONN_STR)
        tdm_1 = TmpDirManager()
        local_file_path = f"{tdm_1.get_folder_name()}/{uuid.uuid4().hex}.mp4"
        blob_manager.download_blob_to_local(config.BlobStorage.SOURCE_CONTAINER, blob_name, local_file_path)
        
        tdm_2 = ffmpeg.split_video(local_file_path, chunk_size)
        split_file_paths = tdm_2.get_files()

        blob_manager.upload_files("chunk", blob_name, split_file_paths)

        producer = common.get_event_hub_producer("translation")
        with producer:
            event_batch = producer.create_batch()
            for idx, path in enumerate(split_file_paths):
                json_data = copy.deepcopy(body)
                json_data.pop("chunk_size")
                json_data["chunk_num"] = len(split_file_paths)
                json_data["chunk_name"] = os.path.basename(path)
                message = json.dumps(json_data)
                event_batch.add(EventData(message))
            producer.send_batch(event_batch)

        return len(split_file_paths)
    except Exception as e:
        logging.error(f"Error in video_service, split_video: {e}")
        raise
    finally:
        os.remove(local_file_path)
        del tdm_1
        del tdm_2

def start_translation(event_body: Dict) -> None:
    lang = event_body.get("lang", {})

    blob_name = event_body.get("blob_name")
    source_lang = lang.get("source")
    target_lang = lang.get("target")
    chunk_name = event_body.get("chunk_name")
    with_subtitle = True if event_body.get("with_subtitle") == "true" else False

    try:
        blob_manager = AzureBlobManager(config.BlobStorage.CONN_STR)
        blob_url = blob_manager.get_blob_url("chunk", f"{blob_name}/{chunk_name}")

        operation_id = uuid.uuid4().hex
        translation_id = uuid.uuid4().hex

        req_body = {
            "displayName": chunk_name,
            "description": "A brief test of the video translation API",
            "input": {
                "sourceLocale": source_lang,
                "targetLocale": target_lang,
                "voiceKind": "PlatformVoice",
                "videoFileUrl": blob_url,
                "exportSubtitleInVideo": with_subtitle
            },
        }
        req_header = {
            "Ocp-Apim-Subscription-Key": config.VideoTranslation.KEY,
            "Content-Type": "application/json",
            "Operation-Id": operation_id
        }
        url = f"{config.VideoTranslation.BASE_URL}/translations/{translation_id}?api-version=2024-05-20-preview"

        response = requests.put(url, headers=req_header, json=req_body)
        logging.info(f"status code {response.status_code}, text: {response.text}")
        response.raise_for_status()

        producer = common.get_event_hub_producer("transoperationcheck")
        with producer:
            event_batch = producer.create_batch()
            json_data = event_body
            json_data.pop("with_subtitle")
            json_data["operation_id"] = operation_id
            json_data["translation_id"] = translation_id
            message = json.dumps(json_data)
            event_batch.add(EventData(message))
            producer.send_batch(event_batch)
            logging.info(f"Send message: {json_data} to event hub transoperationcheck")
    except Exception as e:
        logging.error(f"Error in video_service, start_translation: {e}")
        raise

def start_iteration(event_body: Dict) -> None:
    operation_id = event_body.get("operation_id", None)
    vtt_name = event_body.get("vtt_name", None)
    blob_name = event_body.get("blob_name")

    try:
        if operation_id:
            res = common.fetch_operation_status(operation_id)
            while (res.get("status") != "Succeeded"):
                time.sleep(10)
                res = common.fetch_operation_status(operation_id)
    except Exception as e:
        logging.error(f"Error in video_service, start_iteration, polling phase: {e}")
        raise

    iteration_id = uuid.uuid4().hex
    new_operation_id = uuid.uuid4().hex
    translation_id = event_body.get("translation_id")

    url = f"{config.VideoTranslation.BASE_URL}/translations/{translation_id}/iterations/{iteration_id}?api-version=2024-05-20-preview"
    body = {}
    req_header = {
        "Ocp-Apim-Subscription-Key": config.VideoTranslation.KEY,
        "Content-Type": "application/json",
        "Operation-Id": new_operation_id
    }

    try:
        if vtt_name:
            blob_manager = AzureBlobManager(config.BlobStorage.CONN_STR)
            vtt_url = blob_manager.get_blob_url("transcription", f"{blob_name}/{vtt_name}")
            body["input"] = {
                "webvttFile": {
                    "url": vtt_url,
                    "kind": "MetadataJson"
                }
            }

        response = requests.put(url, headers=req_header, json=body)
        response.raise_for_status()

        producer = common.get_event_hub_producer("iteroperationcheck")
        with producer:
            event_batch = producer.create_batch()
            json_data = event_body
            json_data["operation_id"] = new_operation_id
            json_data["iteration_id"] = iteration_id
            if vtt_name:
                json_data["mode"] = "native"            # downgrade enhancement to native cuz vtt has already well-config
            message = json.dumps(json_data)
            event_batch.add(EventData(message))
            producer.send_batch(event_batch)
            logging.info(f"Send message: {json_data} to event hub iteroperationcheck")
    except Exception as e:
        logging.error(f"Error in video_service, start_iteration: {e}")
        raise

def polling_iteration(event_body: Dict):
    operation_id = event_body.get("operation_id")

    try:
        res = common.fetch_operation_status(operation_id)
        status = res.get("status")
        # Polling job status
        if (status != "Succeeded"):
            # Retry
            if (status == "Failed"):
                logging.info(f"job failed, oper_id: {operation_id}")
                producer = common.get_event_hub_producer("transoperationcheck")

                with producer:
                    event_body.pop("iteration_id")
                    event_body.pop("operation_id")
                    event_batch = producer.create_batch()
                    message = json.dumps(event_body)
                    event_batch.add(EventData(message))
                    producer.send_batch(event_batch)
            # Send back to job queue
            else:
                logging.info(f"job running, oper_id: {operation_id}")
                time.sleep(60)
                producer = common.get_event_hub_producer("iteroperationcheck")
                with producer:
                    event_batch = producer.create_batch()
                    message = json.dumps(event_body)
                    event_batch.add(EventData(message))
                    producer.send_batch(event_batch)
            return
    except Exception as e:
        logging.error(f"Error in video_service, polling_iteration, polling phase: {e}")
        raise
    
    lang = event_body.get("lang", {})

    translation_id = event_body.get("translation_id")
    iteration_id = event_body.get("iteration_id")
    origin_video_name = event_body.get("blob_name")
    chunk_name = event_body.get("chunk_name")
    mode = event_body.get("mode", "native")
    target_lang = lang.get("target")

    try:
        url = f"{config.VideoTranslation.BASE_URL}/translations/{translation_id}/iterations/{iteration_id}?api-version=2024-05-20-preview"
        req_header = {
            "Ocp-Apim-Subscription-Key": config.VideoTranslation.KEY
        }

        response = requests.get(url, headers=req_header)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Error in video_service, polling_iteration, get translated result phase: {e}")
        raise

    if mode == "enhancement":
        local_file_path = f"/tmp/{uuid.uuid4().hex}.vtt"
        vtt_name = chunk_name.split(".")[0] + ".vtt"
        vtt_url = response.json().get("result").get("metadataJsonWebvttFileUrl", "")
        category_id = event_body.get("category_id", None)

        logging.info(f"response: {response.json()}")

        if not vtt_url:
            raise ValueError(f"No vtt url, get response {response.json()}")

        response = requests.get(vtt_url, stream=True)
        response.raise_for_status()

        with open(local_file_path, "w", encoding="utf-8") as file:
            for line in response.iter_lines(decode_unicode=True):
                file.write(line + "\n")
        
        tt = TextTranslator(config.Translator.KEY, config.Translator.ENDPOINT, config.Translator.REGION)
        if category_id:
            tt.set_category_id(category_id)

        tp = TranscriptionParser(target_lang, tt)
        tp.easy_parse(local_file_path)

        blob_manager = AzureBlobManager(config.BlobStorage.CONN_STR)
        blob_name = f"{origin_video_name}/{vtt_name}"
        blob_manager.upload_file("transcription", blob_name, local_file_path)

        producer = common.get_event_hub_producer("transoperationcheck")
        with producer:
            event_body.pop("iteration_id")
            event_body.pop("operation_id")
            event_body["vtt_name"] = vtt_name
            event_batch = producer.create_batch()
            message = json.dumps(event_body)
            event_batch.add(EventData(message))
            producer.send_batch(event_batch)
    else:
        try:
            tdm = TmpDirManager()
            local_file_path = f"{tdm.get_folder_name()}/{uuid.uuid4().hex}.mp4"
            raw_video_url = response.json().get("result").get("translatedVideoFileUrl")

            response = requests.get(raw_video_url, stream=True)
            response.raise_for_status()

            with open(local_file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            blob_manager = AzureBlobManager(config.BlobStorage.CONN_STR)
            blob_name = f"{origin_video_name}/{chunk_name}"
            blob_manager.upload_file("translatedchunk", blob_name, local_file_path)
            return True
        except Exception as e:
            logging.error(f"Error in video_service, polling_iteration, native mode processing phase: {e}")
            raise
        finally:
            del tdm

def merge_video(body: Dict):
    tdm = None
    merged_file_path = None
    try:
        origin_video_name = body.get("blob_name")
        blob_manager = AzureBlobManager(config.BlobStorage.CONN_STR)

        video_cnt = len(blob_manager.list_files_in_folder("translatedchunk", origin_video_name))
        total_cnt = body.get("chunk_num")

        logging.info(f"video_cnt: {video_cnt}, total_cnt: {total_cnt}")
        if (video_cnt == int(total_cnt)):
            tdm =TmpDirManager()
            local_directory_name = tdm.get_folder_name()
            blob_manager.download_directory_to_local("translatedchunk", origin_video_name, local_directory_name)
            merged_file_path = ffmpeg.merge_videos_in_directory(local_directory_name)
            
            blob_manager.upload_file(config.BlobStorage.TARGET_CONTAINER, origin_video_name, merged_file_path)
    except Exception as e:
        logging.error(f"Error in merge_video: {e}")
        raise
    finally:
        if tdm:
            del tdm
        if merged_file_path:
            os.remove(merged_file_path)