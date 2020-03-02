#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import time
import requests
from datetime import datetime
from dateutil import parser
from typing import Union, List, Dict, Iterable
from pathlib import Path
from drepr import DRepr
from drepr.models import SemanticModel
from drepr.outputs import ArrayBackend, GraphBackend
from drepr.outputs.base_lst_output_class import BaseLstOutputClass
from drepr.outputs.base_output_class import BaseOutputClass
from drepr.outputs.base_output_sm import BaseOutputSM
import subprocess

from drepr.outputs.base_record import BaseRecord
from drepr.outputs.record_id import RecordID

from dtran.argtype import ArgType
from dtran.ifunc import IFunc, IFuncType


class DcatReadFunc(IFunc):
    id = "dcat_read_func"
    description = ''' An entry point in the pipeline.
    Fetches a dataset and its metadata from the MINT Data-Catalog.
    '''
    func_type = IFuncType.READER
    friendly_name: str = "Data Catalog Reader"
    inputs = {
        "dataset_id": ArgType.String,
        "start_time": ArgType.DateTime(optional=True),
        "end_time": ArgType.DateTime(optional=True),
        "use_cache": ArgType.Boolean(optional=True)
    }
    outputs = {"data": ArgType.DataSet(None)}
    example = {
        "dataset_id": "05c43c58-ed42-4830-9b1f-f01059c4b96f"
    }

    def __init__(self, dataset_id: str, start_time: datetime = None, end_time: datetime = None, use_cache: bool = True):
        # TODO: move to a diff arch (pointer to Data-Catalog URL)
        DCAT_URL = "https://api.mint-data-catalog.org"

        self.dataset_id = dataset_id
        self.logger = logging.getLogger(DcatReadFunc.id)

        dataset_result = DCatAPI.get_instance(DCAT_URL).find_dataset_by_id(dataset_id)

        assert ('resource_repr' in dataset_result['metadata']) or ('dataset_repr' in dataset_result['metadata']), \
            "Dataset is missing both 'resource_repr' and 'dataset_repr'"
        assert not (('resource_repr' in dataset_result['metadata']) and ('dataset_repr' in dataset_result['metadata'])), \
            "Dataset has both 'resource_repr' and 'dataset_repr'"

        resource_results = DCatAPI.get_instance(DCAT_URL).find_resources_by_dataset_id(dataset_id)
        resource_ids = {}
        if 'resource_repr' in dataset_result['metadata']:
            self.repr = DRepr.parse(dataset_result['metadata']['resource_repr'])
            for resource in resource_results:
                if start_time or end_time:
                    temporal_coverage = resource['resource_metadata']['temporal_coverage']
                    temporal_coverage['start_time'] = parser.parse(temporal_coverage['start_time'])
                    temporal_coverage['end_time'] = parser.parse(temporal_coverage['end_time'])
                    if (start_time and start_time > temporal_coverage['end_time']) or \
                            (end_time and end_time < temporal_coverage['start_time']):
                        continue

                resource_ids[resource['resource_id']] = resource['resource_data_url']
            self.repr_type = 'resource_repr'
        else:
            # TODO: fix me!!
            assert len(resource_results) == 1
            resource_ids[resource_results[0]['resource_id']] = resource_results[0]['resource_data_url']
            self.repr = DRepr.parse(dataset_result['metadata']['dataset_repr'])
            self.repr_type = 'dataset_repr'

        self.logger.debug(f"Found key '{self.repr_type}'")
        Path("/tmp/dcat_read_func").mkdir(exist_ok=True, parents=True)

        self.logger.debug(f"Downloading {len(resource_ids)} resources ...")
        self.resources = {}
        for resource_id, resource_url in resource_ids.items():
            file_full_path = f'/tmp/dcat_read_func/{resource_id}.dat'
            self.resources[resource_id] = file_full_path
            if use_cache and Path(file_full_path).exists():
                self.logger.debug(f"Skipping resource {resource_id}, found in cache")
                continue
            self.logger.debug(f"Downloading resource {resource_id} ...")
            subprocess.check_call(f'wget {resource_url} -O {file_full_path}', shell=True)

        self.logger.debug(f"Download Complete")

    def exec(self) -> dict:
        if self.get_preference("data") is None or self.get_preference("data") == 'array':
            backend = ArrayBackend
        else:
            backend = GraphBackend

        if self.repr_type == 'dataset_repr':
            return {"data": backend.from_drepr(self.repr, list(self.resources.values())[0])}
        else:
            datasets = [None] * len(self.resources)
            for i, resource in enumerate(self.resources.values()):
                datasets[len(datasets) - i - 1] = backend.from_drepr(self.repr, resource)
            return {"data": ShardedBackend(datasets)}

    def validate(self) -> bool:
        return True


class ShardedClassID(str):
    def __new__(cls, index: int, class_id: str):
        return super().__new__(cls, class_id)

    def __init__(self, index: int, class_id: str):
        self.index = index
        self.class_id = class_id


class ShardedBackend(BaseOutputSM):
    def __init__(self, datasets: List[BaseOutputSM]):
        # list of datasets
        self.datasets = datasets
        '''
        for i, dataset in enumerate(self.datasets):
            for output_class in dataset.iter_classes():
                output_class.id = ShardedClassID(i, output_class.id)
        '''

    @classmethod
    def from_drepr(cls, ds_model: Union[DRepr, str], resources: Union[str, Dict[str, str]]) -> BaseOutputSM:
        raise NotImplementedError("This method should never be called")

    def iter_classes(self) -> Iterable[BaseOutputClass]:
        pass

    def get_record_by_id(self, rid: RecordID) -> BaseRecord:
        pass
        '''
        return self.datasets[rid.class_id.index].get_record_by_id(rid)
        '''

    def c(self, class_uri: str) -> BaseLstOutputClass:
        pass

    def cid(self, class_id: str) -> BaseOutputClass:
        pass

    def _get_sm(self) -> SemanticModel:
        pass

    def drain(self):
        """
        Iterate and remove the dataset out of the list. After this function, the list of datasets should be empty
        """
        for i in range(len(self.datasets)):
            yield self.datasets.pop()


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
