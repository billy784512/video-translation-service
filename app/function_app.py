import os
import uuid
import json
import logging
import requests
import time

from azure.functions import HttpRequest, HttpResponse, FunctionApp, EventHubEvent, AuthLevel
from azure.eventhub import EventHubProducerClient, EventData

from ffmpeg_utils import split_video, merge_videos_in_directory
from azure_blob_manager import AzureBlobManager
from transcription_parser import TranscriptionParser

app = FunctionApp()

TRANSLATION_API_KEY = os.getenv("TRANSLATION_API_KEY")
BASE_URL = os.getenv("TRANSLATION_API_URL")
EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")

@app.route(route="video-translate", methods=["POST"], auth_level= AuthLevel.ANONYMOUS)
def video_translation(req: HttpRequest) -> HttpResponse:
    try:
        body = req.get_json()
        logging.info(f"request with body: {body}")
        
        blob = body.get("blob", {})
        lang = body.get("lang", {})

        conn_str = blob.get("conn_str")
        blob_name = blob.get("blob_name")
        source_container_name = blob.get("source_container_name")
        source_lang = lang.get("source")
        mode = body.get("mode", "native")

        if mode not in {"native", "enhancement"}:
            return HttpResponse(
                "The value of mode should be either 'native' or 'enhancement'.",
                status_code=400
            )

        if mode == "native":
            target_url = "https://video-translation-poc.bluestone-e5a829ec.eastus.azurecontainerapps.io/api/video-split?"

            response = requests.post(target_url, json=body)
            response.raise_for_status()

            if response.status_code == 201:
                return HttpResponse(
                    "Video translation sucessfully initiated.",
                    status_code=201
                )
            
        if mode == "enhancement":
            target_url = "https://videotranscriptionqa.ambitiousdune-ee7444fd.swedencentral.azurecontainerapps.io/api/transcription?code=7I3kq-0bMvjsuFQK2UVn3puWEhVufglnhx0WBafJ0DJRAzFuVNGiPA%3D%3D"

            req_body = {
                "video_name": blob_name,
                "blob_connection_string": conn_str,
                "input_container_name": source_container_name,
                "output_container_name": "transcription",
                "locales": [source_lang]
            }

            response = requests.post(target_url, json=req_body)
            response.raise_for_status()

            if response.status_code != 200:
                return HttpResponse(
                    f"Transcription return unexpected status code: {response.status_code}",
                    status_code=400
                )

            # TODO: 若是 zh-TW, 要多 call 一層 translator 作簡轉繁... (開一個新 class 比較好)
            logging.info(f"start json to vtt process...")
        
            blob_manager = AzureBlobManager(conn_str)
            tp = TranscriptionParser()

            local_directory = blob_manager.download_directory_to_local("transcription", blob_name, ".json")
            files = [os.path.join(local_directory, f) for f in sorted(os.listdir(local_directory)) if f.endswith(".json")]

            for idx, path in enumerate(files):
                new_file_path = os.path.dirname(path) + f"/{idx:02}.vtt"
                tp.parse_and_save(path, new_file_path)
                upload_name = f"{blob_name}/{os.path.basename(new_file_path)}"
                blob_manager.upload_file("transcription", upload_name, new_file_path)

            blob_name_list = blob_manager.list_files_in_folder("chunk", blob_name, ".mp4")
            transcription_name_list = blob_manager.list_files_in_folder("transcription", blob_name, ".vtt")

            logging.info(f"blob_name_list: {blob_name_list}")
            logging.info(f"transcription_name_list: {transcription_name_list}")

            if len(blob_name_list) != len(transcription_name_list):
                logging.info(f"Two list size inconsistent, blob: {blob_name_list}, transcription: {transcription_name_list}")
                return HttpResponse(
                    "Two list size inconsistent",
                    status_code=400
                )

            producer = get_event_hub_producer("translation")
            with producer:
                event_batch = producer.create_batch()
                idx, n = 0, len(blob_name_list)
                while idx < n:
                    message = json.dumps({
                        "with_subtitle": body.get("with_subtitle"),
                        "chunk_name": os.path.basename(blob_name_list[idx]),
                        "chunk_num": n,
                        "vtt_name": os.path.basename(transcription_name_list[idx]),
                        "blob": blob,
                        "lang": lang
                    })
                    event_batch.add(EventData(message))
                    idx += 1
                    logging.info(f"message: {message}")
                producer.send_batch(event_batch)

            return HttpResponse(
                "Video translation sucessfully initiated.",
                status_code=201
            )
            
    except requests.RequestException as e:
        logging.error(f"HTTP error: {e}")
        return HttpResponse(
            "Execution failed.",
            status_code=500
        )

@app.route(route="video-split", methods=["POST"], auth_level= AuthLevel.ANONYMOUS)
def video_split(req: HttpRequest) -> HttpResponse:
    try:
        body = req.get_json()

        blob = body.get("blob", {})
        lang = body.get("lang", {})

        source_container_name = blob.get("source_container_name")
        blob_name = blob.get("blob_name")
        conn_str = blob.get("conn_str")
        chunk_size = body.get("chunk_size", 100)
        pass_to_eventhub = body.get("pass_to_eventhub", False)
        with_subtitle = body.get("with_subtitle")

        if not source_container_name or not blob_name or not conn_str:
            return HttpResponse(
                "Please provide 'conn_str', 'source_container_name' and 'blob_name' in the request body.",
                status_code=400
            )

        blob_manager = AzureBlobManager(conn_str)
        local_file_path = blob_manager.download_blob_to_local(source_container_name, blob_name)
        chunked_files = split_video(local_file_path, chunk_size)
        blob_manager.upload_chunks("chunk", blob_name, chunked_files)

        # Clean up temporary files
        os.remove(local_file_path)
        for file in chunked_files:
            os.remove(file)

        if pass_to_eventhub:
            producer = get_event_hub_producer("translation")
            with producer:
                event_batch = producer.create_batch()
                for idx, chunk_path in enumerate(chunked_files):
                    message = json.dumps({
                        "with_subtitle": with_subtitle,
                        "chunk_name": os.path.basename(chunk_path),
                        "chunk_num": len(chunked_files),
                        "blob": blob,
                        "lang": lang
                    })
                    event_batch.add(EventData(message))
                producer.send_batch(event_batch)

        return HttpResponse(
            f"Video processed successfully. {len(chunked_files)} chunks uploaded.",
            status_code=201
        )

    except Exception as e:
        logging.error(f"Error processing video: {e}")
        return HttpResponse(
            "An error occurred while processing the video.",
            status_code=500
        )
    
@app.function_name(name="translation")
@app.event_hub_message_trigger(arg_name="azehub", event_hub_name="translation", connection="EVENT_HUB_CONNECTION_STRING")
def trnslation(azehub: EventHubEvent):
    try:
        event_body = json.loads(azehub.get_body().decode("utf-8"))

        logging.info(f"Receive message: {event_body}")
        
        blob = event_body.get("blob", {})
        lang = event_body.get("lang", {})

        conn_str = blob.get("conn_str")
        blob_name = blob.get("blob_name")
        source_lang = lang.get("source")
        target_lang = lang.get("target")
        chunk_name = event_body.get("chunk_name")
        chunk_num = event_body.get("chunk_num")
        vtt_name = event_body.get("vtt_name")
        with_subtitle = True if event_body.get("with_subtitle") == "true" else False

        blob_manager = AzureBlobManager(conn_str)
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
            "Ocp-Apim-Subscription-Key": TRANSLATION_API_KEY,
            "Content-Type": "application/json",
            "Operation-Id": operation_id
        }
        url = f"{BASE_URL}/translations/{translation_id}?api-version=2024-05-20-preview"

        response = requests.put(url, headers=req_header, json=req_body)
        response.raise_for_status()

        producer = get_event_hub_producer("transoperationcheck")
        with producer:
            event_batch = producer.create_batch()
            json_data = {
                "chunk_name": chunk_name,
                "chunk_num": chunk_num,
                "blob": blob,
                "lang": lang,
                "operation_id": operation_id,
                "translation_id": translation_id
            }

            if vtt_name:
                json_data["vtt_name"] = vtt_name

            message = json.dumps(json_data)
            event_batch.add(EventData(message))
            producer.send_batch(event_batch)
            logging.info(f"Send message: {json_data} to event hub transoperationcheck")

    except Exception as e:
        logging.error(f"Error in translation: {e}")
        raise

@app.function_name(name="iteration")
@app.event_hub_message_trigger(arg_name="azehub", event_hub_name="transoperationcheck", connection="EVENT_HUB_CONNECTION_STRING")
def iteration(azehub: EventHubEvent):
    try:
        # Decode and log the event body
        event_body = json.loads(azehub.get_body().decode("utf-8"))

        logging.info(f"Receive message: {event_body}")

        blob = event_body.get("blob", {})
        lang = event_body.get("lang", {})

        operation_id = event_body.get("operation_id", None)
        vtt_name = event_body.get("vtt_name", "")
        conn_str = blob.get("conn_str")
        blob_name = blob.get("blob_name")

        if operation_id:
            res = fetch_translation_status(operation_id)
            while (res.get("status") != "Succeeded"):
                time.sleep(10)
                response = requests.get(url, headers=req_header)
                response.raise_for_status()

        logging.info(f"Create iteration now...")

        iteration_id = uuid.uuid4().hex
        operation_id = uuid.uuid4().hex
        translation_id = event_body.get("translation_id")
        url = f"{BASE_URL}/translations/{translation_id}/iterations/{iteration_id}?api-version=2024-05-20-preview"

        req_header = {
            "Ocp-Apim-Subscription-Key": TRANSLATION_API_KEY,
            "Content-Type": "application/json",
            "Operation-Id": operation_id
        }

        body = {}
        if vtt_name:
            blob_manager = AzureBlobManager(conn_str)
            vtt_url = blob_manager.get_blob_url("transcription", f"{blob_name}/{vtt_name}")
            body["input"] = {
                "webvttFile": {
                    "url": vtt_url,
                    "kind": "MetadataJson"
                }
            }

        logging.info(f"url: {url}, req_header: {req_header}, body: {body}")
        response = requests.put(url, headers=req_header, json=body)

        if not (response.status_code == 200 or response.status_code == 201):
            logging.info(f"Error: {response.status_code}, {response.text}")

        response.raise_for_status()
        logging.info(f"Create iteration successfully")

        producer = get_event_hub_producer("iteroperationcheck")

        with producer:
            event_batch = producer.create_batch()
            json_data = {
                "chunk_name": event_body.get("chunk_name"),
                "chunk_num": event_body.get("chunk_num"),
                "blob": blob,
                "lang": lang,
                "translation_id": translation_id,
                "iteration_id": iteration_id,
                "operation_id": operation_id
            }

            if vtt_name:
                json_data["vtt_name"] = vtt_name

            message = json.dumps(json_data)
            event_batch.add(EventData(message))
            producer.send_batch(event_batch)
            logging.info(f"Send message: {json_data} to event hub iteroperationcheck")
                    
    except requests.exceptions.RequestException as req_err:
        logging.error(f"HTTP request error: {req_err}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in 'translation' function: {e}")
        raise

@app.function_name(name="iteration-check")
@app.event_hub_message_trigger(arg_name="azehub", event_hub_name="iteroperationcheck", connection="EVENT_HUB_CONNECTION_STRING")
def iteration_check(azehub: EventHubEvent):
    try:
        event_body = json.loads(azehub.get_body().decode("utf-8"))

        logging.info(f"Receive message: {event_body}")

        operation_id = event_body.get("operation_id")

        res = fetch_translation_status(operation_id)
        status = res.get("status")
        if (status != "Succeeded"):
            if (status == "Failed"):
                logging.info(f"job failed, oper_id: {operation_id}")
                producer = get_event_hub_producer("transoperationcheck")

                with producer:
                    event_body.pop("iteration_id")
                    event_body.pop("operation_id")
                    event_batch = producer.create_batch()
                    message = json.dumps(event_body)
                    event_batch.add(EventData(message))
                    producer.send_batch(event_batch)
            else:
                logging.info(f"job running, oper_id: {operation_id}")
                time.sleep(60)
                producer = get_event_hub_producer("iteroperationcheck")
                with producer:
                    event_batch = producer.create_batch()
                    message = json.dumps(event_body)
                    event_batch.add(EventData(message))
                    producer.send_batch(event_batch)
        else:
            blob = event_body.get("blob", {})

            translation_id = event_body.get("translation_id")
            iteration_id = event_body.get("iteration_id")
            conn_str = blob.get("conn_str")
            origin_video_name = blob.get("blob_name")
            chunk_name = event_body.get("chunk_name")
            chunk_num = event_body.get("chunk_num")


            url = f"{BASE_URL}/translations/{translation_id}/iterations/{iteration_id}?api-version=2024-05-20-preview"
            req_header = {
                "Ocp-Apim-Subscription-Key": TRANSLATION_API_KEY
            }

            response = requests.get(url, headers=req_header)

            local_file_path = f"/tmp/{uuid.uuid4().hex}.mp4"
            raw_video_url = response.json().get("result").get("translatedVideoFileUrl")

            response = requests.get(raw_video_url, stream=True)
            response.raise_for_status()

            with open(local_file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            blob_manager = AzureBlobManager(conn_str)
            blob_manager.upload_chunk("translatedchunk", origin_video_name, chunk_name, local_file_path)

            target_url = "https://video-translation-poc.bluestone-e5a829ec.eastus.azurecontainerapps.io/api/video-merge?"
            body = {
                "blob": blob, 
                "chunk_num": chunk_num
            }
            response = requests.post(target_url, json=body)
            response.raise_for_status()
                    
    except requests.exceptions.RequestException as req_err:
        logging.error(f"HTTP request error: {req_err}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in 'translation' function: {e}")
        raise

@app.route(route="video-merge", methods=["POST"], auth_level= AuthLevel.ANONYMOUS)
def video_merge(req: HttpRequest) -> HttpResponse:
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

            return HttpResponse(
                "Video merge done",
                status_code=201
            )
        else:
            return HttpResponse(
                "Still waiting for all video iteration done...",
                status_code=200
            )

    except requests.RequestException as e:
        logging.error(f"Response content: {e.response.text}")
        return HttpResponse(
            "Execution is failed...",
            status_code=500
        )
    
def get_event_hub_producer(event_hub_name: str) -> EventHubProducerClient:
    if not EVENT_HUB_CONNECTION_STRING:
        raise ValueError("EVENT_HUB_CONNECTION_STRING is not set")
    return EventHubProducerClient.from_connection_string(EVENT_HUB_CONNECTION_STRING, eventhub_name=event_hub_name)

def fetch_translation_status(operation_id: str) -> dict:
    url = f"{BASE_URL}/operations/{operation_id}?api-version=2024-05-20-preview"
    headers = {"Ocp-Apim-Subscription-Key": TRANSLATION_API_KEY}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()
