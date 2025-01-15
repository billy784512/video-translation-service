import os
import uuid
import json
import logging
import requests
import time

import azure.functions as func
from azure.eventhub import EventHubProducerClient, EventData

from app.ffmpeg_utils import split_video, merge_videos_in_directory
from AzureBlobManager import AzureBlobManager

app = func.FunctionApp()

@app.route(route="video-translate", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def video_translation(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        mode = body.get("mode", "native")

        target_url = ""

        if mode == "native":
            target_url = "https://video-translation-poc.bluestone-e5a829ec.eastus.azurecontainerapps.io/api/video-split?"
        elif mode == "enhancement":
            target_url = ""
        else:
            return func.HttpResponse(
                "The value of mode should be either \"native\" or \"enhancement\".",
                status_code=400
            )
        
        logging.info("Calling video split.")
        response = requests.post(target_url, json=body)
        response.raise_for_status()

        if response.status_code == 201:
            return func.HttpResponse(
                "Video translation sucessfully initiated.",
                status_code=201
            )
    except requests.RequestException as e:
        logging.error(f"Response content: {e.response.text}")
        return func.HttpResponse(
            "Execution is failed...",
            status_code=500
        )

@app.route(route="video-split", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def video_split(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        source_container_name = body.get("blob").get("source_container_name")
        target_container_name = "chunk"
        blob_name = body.get("blob").get("blob_name")
        conn_str = body.get("blob").get("conn_str")
        chunk_size = body.get("chunk_size", 450)  # Default chunking size: 450 MB
        pass_to_eventhub = body.get("pass_to_eventhub", True)

        if not source_container_name or not blob_name:
            return func.HttpResponse(
                "Please provide 'source_container_name' and 'blob_name' in the request body.",
                status_code=400
            )
        
        blob_manager = AzureBlobManager(conn_str)
        
        local_file_path = blob_manager.download_blob_to_local(source_container_name, blob_name)
        chunked_files = split_video(local_file_path, chunk_size)
        blob_manager.upload_chunks(target_container_name, blob_name, chunked_files)

        # Remove tmp file
        os.remove(local_file_path)
        for file in chunked_files:
            os.remove(file)

        if pass_to_eventhub:
            EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
            EVENT_HUB_NAME="translation"

            producer = EventHubProducerClient.from_connection_string(
                conn_str=EVENT_HUB_CONNECTION_STRING,
                eventhub_name=EVENT_HUB_NAME
            )

            with producer:
                event_batch = producer.create_batch()
                for idx, chunk_path in enumerate(chunked_files):
                    json_data = {
                        "chunk_name": os.path.basename(chunk_path),
                        "chunk_num": len(chunked_files),
                        "blob": body.get("blob"),
                        "lang": body.get("lang")
                    }
                    message = json.dumps(json_data)
                    event_batch.add(EventData(message))
                producer.send_batch(event_batch)

        return func.HttpResponse(
            f"Video processed successfully. {len(chunked_files)} chunks uploaded.",
            status_code=201
        )
    except Exception as e:
        logging.error(f"Error processing video: {e}")
        return func.HttpResponse(
            "An error occurred while processing the video.",
            status_code=500
        )
    
@app.function_name(name="translation")
@app.event_hub_message_trigger(arg_name="azehub", event_hub_name="translation", connection="EVENT_HUB_CONNECTION_STRING")
def trnslation(azehub: func.EventHubEvent):
    try:
        operation_id = uuid.uuid4().hex
        translation_id = uuid.uuid4().hex
        logging.info(f"Generated operation_id: {operation_id}, translation_id: {translation_id}")

        # Decode and log the event body
        event_body = json.loads(azehub.get_body().decode("utf-8"))

        conn_str = event_body.get("blob").get("conn_str")
        blob_name = event_body.get("blob").get("blob_name")
        chunk_name = event_body.get("chunk_name")
        source_lang = event_body.get("lang").get("source")
        target_lang = event_body.get("lang").get("target")

        # Initialize blob manager and retrieve blob URL
        blob_manager = AzureBlobManager(conn_str)
        blob_url = blob_manager.get_blob_url("chunk", f"{blob_name}/{chunk_name}")

        KEY = os.getenv("TRANSLATION_API_KEY")
        # Prepare request body and headers
        req_body = {
            "displayName": chunk_name,
            "description": "A brief test of the video translation API",
            "input": {
                "sourceLocale": source_lang,
                "targetLocale": target_lang,
                "voiceKind": "PlatformVoice",
                "videoFileUrl": blob_url
            },
        }
        req_header = {
            "Ocp-Apim-Subscription-Key": KEY,
            "Content-Type": "application/json",
            "Operation-Id": operation_id
        }

        BASE_URL = os.getenv("TRANSLATION_API_URL")
        url = f"{BASE_URL}/translations/{translation_id}?api-version=2024-05-20-preview"

        # Send the request to the translation API
        response = requests.put(url, headers=req_header, json=req_body)
        response.raise_for_status()

        # Prepare and send message to Event Hub
        EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
        EVENT_HUB_NAME = "transoperationcheck"

        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STRING,
            eventhub_name=EVENT_HUB_NAME
        )

        with producer:
            event_batch = producer.create_batch()
            json_data = {
                "chunk_name": chunk_name,
                "chunk_num": event_body.get("chunk_num"),
                "blob": event_body.get("blob"),
                "lang": event_body.get("lang"),
                "operation_id": operation_id,
                "translation_id": translation_id
            }
            message = json.dumps(json_data)
            event_batch.add(EventData(message))
            producer.send_batch(event_batch)

    except requests.exceptions.RequestException as req_err:
        logging.error(f"HTTP request error: {req_err}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in 'translation' function: {e}")
        raise

@app.function_name(name="iteration")
@app.event_hub_message_trigger(arg_name="azehub", event_hub_name="transoperationcheck", connection="EVENT_HUB_CONNECTION_STRING")
def iteration(azehub: func.EventHubEvent):
    try:
        # Decode and log the event body
        event_body = json.loads(azehub.get_body().decode("utf-8"))

        logging.info(f"Receive message: {event_body}")

        operation_id = event_body.get("operation_id")

        KEY = os.getenv("TRANSLATION_API_KEY")
        req_header = {
            "Ocp-Apim-Subscription-Key": KEY,
        }

        BASE_URL = os.getenv("TRANSLATION_API_URL")
        url = f"{BASE_URL}/operations/{operation_id}?api-version=2024-05-20-preview"

        response = requests.get(url, headers=req_header)
        response.raise_for_status()

        while (response.json().get("status") != "Succeeded"):
            time.sleep(10)
            response = requests.get(url, headers=req_header)
            response.raise_for_status()

        logging.info(f"Create iteration now...")

        iteration_id = uuid.uuid4().hex
        operation_id = uuid.uuid4().hex
        translation_id = event_body.get("translation_id")
        url = f"{BASE_URL}/translations/{translation_id}/iterations/{iteration_id}?api-version=2024-05-20-preview"

        req_header = {
            "Ocp-Apim-Subscription-Key": KEY,
            "Content-Type": "application/json",
            "Operation-Id": operation_id
        }
        
        response = requests.put(url, headers=req_header, json={})
        response.raise_for_status()

        logging.info(f"Create iteration successfully")

        EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
        EVENT_HUB_NAME = "iteroperationcheck"

        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STRING,
            eventhub_name=EVENT_HUB_NAME
        )

        with producer:
            event_batch = producer.create_batch()
            json_data = {
                "chunk_name": event_body.get("chunk_name"),
                "chunk_num": event_body.get("chunk_num"),
                "blob": event_body.get("blob"),
                "lang": event_body.get("lang"),
                "translation_id": translation_id,
                "iteration_id": iteration_id,
                "operation_id": operation_id
            }
            message = json.dumps(json_data)
            event_batch.add(EventData(message))
            producer.send_batch(event_batch)
            logging.info(f"Send message: {json_data} to event hub {EVENT_HUB_NAME}")
                    
    except requests.exceptions.RequestException as req_err:
        logging.error(f"HTTP request error: {req_err}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in 'translation' function: {e}")
        raise

@app.function_name(name="iteration-check")
@app.event_hub_message_trigger(arg_name="azehub", event_hub_name="iteroperationcheck", connection="EVENT_HUB_CONNECTION_STRING")
def iteration_check(azehub: func.EventHubEvent):
    BASE_URL = os.getenv("TRANSLATION_API_URL")

    try:
        # Decode and log the event body
        event_body = json.loads(azehub.get_body().decode("utf-8"))

        logging.info(f"Receive message: {event_body}")

        operation_id = event_body.get("operation_id")

        KEY = os.getenv("TRANSLATION_API_KEY")
        req_header = {
            "Ocp-Apim-Subscription-Key": KEY,
        }

        url = f"{BASE_URL}/operations/{operation_id}?api-version=2024-05-20-preview"

        response = requests.get(url, headers=req_header)
        response.raise_for_status()

        if (response.json().get("status") != "Succeeded"):
            time.sleep(30)
            response = requests.get(url, headers=req_header)

            EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
            EVENT_HUB_NAME = "iteroperationcheck"

            producer = EventHubProducerClient.from_connection_string(
                conn_str=EVENT_HUB_CONNECTION_STRING,
                eventhub_name=EVENT_HUB_NAME
            )

            with producer:
                event_batch = producer.create_batch()
                message = json.dumps(event_body)
                event_batch.add(EventData(message))
                producer.send_batch(event_batch)
        else:
            translation_id = event_body.get("translation_id")
            iteration_id = event_body.get("iteration_id")

            url = f"{BASE_URL}/translations/{translation_id}/iterations/{iteration_id}?api-version=2024-05-20-preview"
            req_header = {
                "Ocp-Apim-Subscription-Key": KEY
            }

            response = requests.get(url, headers=req_header)

            local_file_path = f"/tmp/{uuid.uuid4().hex}.mp4"
            raw_video_url = response.json().get("result").get("translatedVideoFileUrl")

            response = requests.get(raw_video_url, stream=True)
            response.raise_for_status()

            with open(local_file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            conn_str = event_body.get("blob").get("conn_str")
            origin_video_name = event_body.get("blob").get("blob_name")
            chunk_name = event_body.get("chunk_name")

            blob_manager = AzureBlobManager(conn_str)
            blob_manager.upload_chunk("translatedchunk", origin_video_name, chunk_name, local_file_path)

            target_url = "https://video-translation-poc.bluestone-e5a829ec.eastus.azurecontainerapps.io/api/video-merge?"
            body = {
                "blob": event_body.get("blob"), 
                "chunk_num": event_body.get("chunk_num")
            }
            response = requests.post(target_url, json=body)
            response.raise_for_status()
                    
    except requests.exceptions.RequestException as req_err:
        logging.error(f"HTTP request error: {req_err}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in 'translation' function: {e}")
        raise

@app.route(route="video-merge", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def video_merge(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        conn_str = body.get("blob").get("conn_str")
        origin_video_name = body.get("blob").get("blob_name")
        
        blob_manager = AzureBlobManager(conn_str)

        video_cnt = blob_manager.count_files_in_folder("translatedchunk", origin_video_name)
        total_cnt = body.get("chunk_num")

        logging.info(f"video_cnt: {video_cnt}, total_cnt: {total_cnt}")
        if (video_cnt == int(total_cnt)):
            local_directory = blob_manager.download_directory_to_local("translatedchunk", origin_video_name)
            merged_file_path = merge_videos_in_directory(local_directory)
            target_container_name = body.get("blob").get("target_container_name")
            blob_manager.upload_file(target_container_name, origin_video_name, merged_file_path)

            os.remove(merged_file_path)

            return func.HttpResponse(
                "Video merge done",
                status_code=201
            )
        else:
            return func.HttpResponse(
                "Still waiting for all video iteration done...",
                status_code=200
            )

    except requests.RequestException as e:
        logging.error(f"Response content: {e.response.text}")
        return func.HttpResponse(
            "Execution is failed...",
            status_code=500
        )
