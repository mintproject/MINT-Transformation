#!/usr/bin/python
# -*- coding: utf-8 -*-

import ujson, logging, time

import requests
import datetime
from typing import Union, List, Dict
from pathlib import Path
from drepr import Graph, DRepr
import subprocess

from dtran.argtype import ArgType
from dtran.ifunc import IFunc, IFuncType


class DcatReadFunc(IFunc):
    id = "dcat_read_func"
    description = ''' An entry point in the pipeline.
    Fetches a dataset and its metadata from the MINT Data-Catalog.
    '''
    func_type = IFuncType.READER
    inputs = {"dataset_id": ArgType.String}
    outputs = {"data": ArgType.Graph(None)}

    def __init__(self, dataset_id: str):
        # TODO: move to a diff arch (pointer to Data-Catalog URL)
        DCAT_URL = "https://api.mint-data-catalog.org"

        self.dataset_id  = dataset_id

        resource_results = DCatAPI.get_instance(DCAT_URL).find_resources_by_dataset_id(dataset_id)
        # TODO: fix me!!
        assert len(resource_results) == 1
        resource_ids = {"default": resource_results[0]['resource_data_url']}
        Path("/tmp/dcat_read_func").mkdir(exist_ok=True, parents=True)

        self.resources = {}
        for resource_id, resource_url in resource_ids.items():
            file_full_path = f'/tmp/dcat_read_func/{resource_id}.dat'
            subprocess.check_call(f'wget {resource_url} -O {file_full_path}', shell=True)
            self.resources[resource_id] = file_full_path

        dataset_result = DCatAPI.get_instance(DCAT_URL).find_dataset_by_id(dataset_id)
        self.repr = DRepr.parse(dataset_result['metadata']['layout'])


    def exec(self) -> dict:
            g = Graph.from_drepr(self.repr, self.resources)
            return {"data": g}

    def validate(self) -> bool:
        return True


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

    def find_resources_by_dataset_id(self, dataset_id: str):
        request_headers = {'Content-Type': "application/json", 'X-Api-Key': self.get_api_key()}
        resp = requests.post(f"{self.dcat_url}/datasets/dataset_resources",
                             headers=request_headers,
                             json={
                                "dataset_id": dataset_id
                             })
        assert resp.status_code == 200, resp.text
        return resp.json()['resources']

    def find_dataset_by_id(self, dataset_id):
        request_headers = {'Content-Type': "application/json", 'X-Api-Key': self.get_api_key()}
        resp = requests.post(f"{self.dcat_url}/datasets/get_dataset_info",
                             headers=request_headers,
                             json={
                                 "dataset_id": dataset_id
                             })
        assert resp.status_code == 200, resp.text
        return resp.json()

    def get_api_key(self):
        """
        :return:
        """
        if self.api_key is None or time.time() - self.api_key['time'] > 600:
            # Obtaining the API Key which allows us to make posts
            resp = requests.get(f"{self.dcat_url}/get_session_token").json()
            self.api_key = {'key': resp['X-Api-Key'], "time": time.time()}
        return self.api_key['key']

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
            now = datetime.datetime.utcnow().replace(microsecond=0).isoformat()
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


# if __name__ == "__main__":
#     dcat = DCatAPI.get_instance("https://api.mint-data-catalog.org")
#     print(dcat.find_dataset_by_id("f6025d61-3dfd-4d7d-9e2c-a8c22f2ed2ec"))