import json
import logging
import requests

from azure.functions import HttpRequest, HttpResponse, FunctionApp, AuthLevel, EventHubEvent

from services import video_service

app = FunctionApp()

@app.route(route="video-translate", methods=["POST"], auth_level= AuthLevel.FUNCTION)
def video_translation(req: HttpRequest) -> HttpResponse:
    try:
        logging.info("video_translation executed")
        body = req.get_json()
        logging.info(f"request with body: {body}")
        
        mode = body.get("mode", "native")
        if mode not in {"native", "enhancement"}:
            return HttpResponse(
                "The value of mode should be either 'native' or 'enhancement'.",
                status_code=400
            )
        
        file_num = video_service.split_video(body)


        return HttpResponse(
            f"Video translation sucessfully initiated. split into {file_num} jobs",
            status_code=201
        )            
    except requests.RequestException as e:
        logging.error(f"HTTP error: {e}")
        return HttpResponse(
            f"Execution failed. {e}",
            status_code=500
        )
    

# @app.route(route="video-split", methods=["POST"], auth_level= AuthLevel.FUNCTION)
# def video_split(req: HttpRequest) -> HttpResponse:
#     try:
#         logging.info("video_split executed")
#         body = req.get_json()
#         logging.info(f"request with body: {body}")

#         file_num = video_service.split_video(body)

#         return HttpResponse(
#             f"Video processed successfully. {file_num} chunks uploaded.",
#             status_code=201
#         )
    
#     except Exception as e:
#         logging.error(f"Error processing video: {e}")
#         return HttpResponse(
#             "An error occurred while processing the video.",
#             status_code=500
#         )
    
@app.function_name(name="translation")
@app.event_hub_message_trigger(arg_name="azehub", event_hub_name="translation", connection="EVENT_HUB_CONNECTION_STRING")
def trnslation(azehub: EventHubEvent):
    try:
        logging.info("trnslation executed")
        event_body = json.loads(azehub.get_body().decode("utf-8"))
        logging.info(f"Receive message: {event_body}")

        video_service.start_translation(event_body)

    except Exception as e:
        logging.error(f"Error in translation: {e}")
        raise

@app.function_name(name="iteration")
@app.event_hub_message_trigger(arg_name="azehub", event_hub_name="transoperationcheck", connection="EVENT_HUB_CONNECTION_STRING")
def iteration(azehub: EventHubEvent):
    try:
        logging.info("iteration executed")
        event_body = json.loads(azehub.get_body().decode("utf-8"))
        logging.info(f"Receive message: {event_body}")

        video_service.start_iteration(event_body)
                    
    except requests.exceptions.RequestException as req_err:
        logging.error(f"HTTP request error: {req_err}")
        raise
    except Exception as e:
        logging.error(f"Error in iteration: {e}")
        raise

@app.function_name(name="iteration-check")
@app.event_hub_message_trigger(arg_name="azehub", event_hub_name="iteroperationcheck", connection="EVENT_HUB_CONNECTION_STRING")
def iteration_check(azehub: EventHubEvent):
    try:
        logging.info("iteration-check executed")
        event_body = json.loads(azehub.get_body().decode("utf-8"))
        logging.info(f"Receive message: {event_body}")
        video_service.polling_iteration(event_body)
        if event_body.get("mode", "native") == "native":
            video_service.merge_video(event_body)
            
    except Exception as e:
        logging.error(f"Error in iteration_check: {e}")
        raise