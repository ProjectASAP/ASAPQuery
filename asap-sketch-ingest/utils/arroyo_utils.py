import json
import time
from typing import List

from utils import http_utils


def get_all_pipelines(arroyo_url: str) -> List[str]:
    # list all pipelines
    response = http_utils.make_api_request(
        url=f"{arroyo_url}/pipelines",
        method="get",
    )
    response = json.loads(response)
    if response["data"] is None:
        print("No pipelines found")
        return []

    pipeline_ids = [pipeline["id"] for pipeline in response["data"]]
    return pipeline_ids


def stop_and_delete_pipelines(
    arroyo_url: str, pipeline_ids: List[str], num_retries: int = 30
):
    # stop each pipeline
    for pipeline_id in pipeline_ids:
        response = http_utils.make_api_request(
            url=f"{arroyo_url}/pipelines/{pipeline_id}",
            method="patch",
            data=json.dumps({"stop": "immediate"}),
        )
        print("Sent stop request for pipeline:", pipeline_id)

    # for each pipeline, get status and verify that stop==immediate and actionInProgress==False
    # for pipelines not satisfying this, retry N times with a delay, before raising an error
    for pipeline_id in pipeline_ids:
        for attempt in range(num_retries):
            try:
                response = http_utils.make_api_request(
                    url=f"{arroyo_url}/pipelines/{pipeline_id}",
                    method="get",
                )
                print("Got status for pipeline:", pipeline_id)

                try:
                    data = json.loads(response)
                    print("data['stop']:", data["stop"], type(data["stop"]))
                    print(
                        "data['actionInProgress']:",
                        data["actionInProgress"],
                        type(data["actionInProgress"]),
                    )
                    if data["stop"] == "immediate" and not data["actionInProgress"]:
                        break
                except json.JSONDecodeError as e:
                    print("Failed to decode JSON response:", e)
                    pass
                time.sleep(10)
            except Exception as e:
                if attempt < num_retries - 1:
                    continue
                else:
                    raise e

    # delete each pipeline
    for pipeline_id in pipeline_ids:
        response = http_utils.make_api_request(
            url=f"{arroyo_url}/pipelines/{pipeline_id}",
            method="delete",
        )
        print("Sent delete request for pipeline:", pipeline_id)
