import logging
import time
from datetime import datetime

import requests


class DCatAPI:
    instance = None
    logger = logging.getLogger("dcat_api")
    logger_api_resp = logging.getLogger("dcat_api.handle_response")

    def __init__(self, dcat_url: str):
        self.dcat_url = dcat_url
        self.api_key = None

    @staticmethod
    def get_instance(dcat_url: str):
        if DCatAPI.instance is None:
            DCatAPI.instance = DCatAPI(dcat_url)
        return DCatAPI.instance

    def find_resources_by_dataset_id(
        self,
        dataset_id: str,
        start_time: datetime = None,
        end_time: datetime = None,
        limit: int = 100000,
    ):
        """start_time and end_time is inclusive"""
        request_headers = {
            "Content-Type": "application/json",
            "X-Api-Key": self.get_api_key(),
        }
        query = {
            "dataset_id": dataset_id,
            "limit": limit,
        }
        if start_time is not None or end_time is not None:
            query["filter"] = {}
            if start_time is not None:
                query["filter"]["start_time__gte"] = start_time.isoformat()
            if end_time is not None:
                query["filter"]["end_time__lte"] = end_time.isoformat()

        resp = requests.post(
            f"{self.dcat_url}/datasets/dataset_resources",
            headers=request_headers,
            json=query,
        )
        assert resp.status_code == 200, resp.text
        return resp.json()["resources"]

    def find_dataset_by_id(self, dataset_id):
        request_headers = {
            "Content-Type": "application/json",
            "X-Api-Key": self.get_api_key(),
        }
        resp = requests.post(
            f"{self.dcat_url}/datasets/get_dataset_info",
            headers=request_headers,
            json={"dataset_id": dataset_id},
        )
        assert resp.status_code == 200, resp.text
        return resp.json()

    def get_api_key(self):
        """
        :return:
        """
        if self.api_key is None or time.time() - self.api_key["time"] > 600:
            # Obtaining the API Key which allows us to make posts
            resp = requests.get(f"{self.dcat_url}/get_session_token").json()
            self.api_key = {"key": resp["X-Api-Key"], "time": time.time()}
        return self.api_key["key"]

    @staticmethod
    def handle_api_response(response: requests.Response):
        """
        This is a convenience method to handle api responses
        :param response:
        :param print_response:
        :return:
        """
        parsed_response = response.json()
        DCatAPI.logger_api_resp.debug("API Response: %s", parsed_response)

        if response.status_code == 200:
            return parsed_response
        elif response.status_code == 400:
            raise Exception("Bad request: %s" % response.text)
        elif response.status_code == 403:
            msg = "Please make sure your request headers include X-Api-Key and that you are using correct url"
            raise Exception(msg)
        else:
            now = datetime.utcnow().replace(microsecond=0).isoformat()
            msg = f"""\n\n
            ------------------------------------- BEGIN ERROR MESSAGE -----------------------------------------
            It seems our server encountered an error which it doesn't know how to handle yet. 
            This sometimes happens with unexpected input(s). In order to help us diagnose and resolve the issue, 
            could you please fill out the following information and email the entire message between ----- to
            danf@usc.edu:
            1) URL of notebook (of using the one from https://hub.mybinder.org/...): [*****PLEASE INSERT ONE HERE*****]
            2) Snapshot/picture of the cell that resulted in this error: [*****PLEASE INSERT ONE HERE*****]

            Thank you and we apologize for any inconvenience. We'll get back to you as soon as possible!

            Sincerely, 
            Dan Feldman

            Automatically generated summary:
            - Time of occurrence: {now}
            - Request method + url: {response.request.method} - {response.request.url}
            - Request headers: {response.request.headers}
            - Request body: {response.request.body}
            - Response: {parsed_response}

            --------------------------------------- END ERROR MESSAGE ------------------------------------------
            \n\n
            """

            raise Exception(msg)
