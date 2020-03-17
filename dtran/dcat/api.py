import logging
import time
from datetime import datetime
from typing import List, Dict

import requests


class StandardVariableForm:
    def __init__(self, ontology: str, name: str, uri: str):
        self.ontology = ontology
        self.name = name
        self.uri = uri


class DCatAPI:
    instance = None
    logger = logging.getLogger("dcat_api")
    logger_api_resp = logging.getLogger("dcat_api.handle_response")

    BATCH_SIZE = 200

    def __init__(self, dcat_url: str):
        self.dcat_url = dcat_url
        self.api_key = None

    @staticmethod
    def get_instance(dcat_url: str):
        if DCatAPI.instance is None:
            DCatAPI.instance = DCatAPI(dcat_url)
        return DCatAPI.instance

    def delete_datasets(self, provenance_id, dataset_ids):
        request_headers = {"Content-Type": "application/json", "X-Api-Key": self.get_api_key()}

        deleted_ids = []
        # Walking through each resource in dataset_obj
        for resource_id in dataset_ids:
            resource_defs = {
                "provenance_id": provenance_id,
                "dataset_id": resource_id,
            }

            resp = requests.post(
                f"{self.dcat_url}/datasets/delete_dataset", headers=request_headers, json=resource_defs
            )

            parsed_response = DCatAPI.handle_api_response(resp)

            print(f"{resource_id}: {parsed_response}")

        return deleted_ids

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

    def update_dataset_metadata(self, provenance_id, dataset_id, metadata):
        request_headers = {"Content-Type": "application/json", "X-Api-Key": self.get_api_key()}
        resp = requests.post(
            f"{self.dcat_url}/datasets/update_dataset",
            headers=request_headers,
            json={
                "dataset_id": dataset_id,
                "provenance_id": provenance_id,
                "metadata": metadata
            },
        )
        assert resp.status_code == 200, resp.text
        parsed_response = DCatAPI.handle_api_response(resp)
        return parsed_response

    def register_dataset_with_multiple_resources(
        self,
        provenance_id: str,
        name: str,
        description: str,
        variables: List[Dict[str, str]],
        resources_metadata: List[Dict],
        record_id=None,
        dataset_metadata=None,
    ):
        request_headers = {"Content-Type": "application/json", "X-Api-Key": self.get_api_key()}

        # TODO: register a dataset
        dataset = {
            "provenance_id": provenance_id,
            "name": name,
            "description": description,
            "metadata": dataset_metadata or {}
        }
        if record_id is not None:
            dataset["record_id"] = record_id
        resp = requests.post(
            f"{self.dcat_url}/datasets/register_datasets",
            headers=request_headers,
            json={"datasets": [dataset]},
        )

        parsed_response = DCatAPI.handle_api_response(resp)
        dataset["record_id"] = parsed_response["datasets"][0]["record_id"]
        self.logger.debug("register dataset: %s", dataset)

        # TODO: register all variables
        resp = requests.post(
            f"{self.dcat_url}/datasets/register_variables",
            headers=request_headers,
            json={
                "variables": [
                    {
                        "dataset_id": dataset["record_id"],
                        "name": var["name"],
                        "metadata": var["metadata"],
                        "standard_variable_ids": var["standard_name_id"],
                    }
                    for var in variables
                ]
            },
        )
        variables = DCatAPI.handle_api_response(resp)["variables"]
        self.logger.debug("register variables: %s", variables)

        # Register resources
        num_resources = len(resources_metadata)
        num_steps = num_resources // self.BATCH_SIZE + 1
        variable_records_ids = [v["record_id"] for v in variables]

        for step in range(num_steps):
            start_idx = step * self.BATCH_SIZE
            if (step + 1) * self.BATCH_SIZE > num_resources:
                last_idx = num_resources
            else:
                last_idx = (step + 1) * self.BATCH_SIZE
            resources = resources_metadata[start_idx:last_idx]

            # TODO: register all resources
            resource_defs = {"resources": []}
            # Walking through each resource in dataset_obj
            for resource in resources:
                resource_defs["resources"].append(
                    {
                        "provenance_id": provenance_id,
                        "dataset_id": dataset["record_id"],
                        "variable_ids": variable_records_ids,
                        "name": resource["name"],
                        "resource_type": resource["resource_type"],
                        "data_url": resource["data_url"],
                        "metadata": resource["metadata"],
                    }
                )

            # self.logger.debug(f"register resources index {start_idx} to {last_idx - 1} ")
            print(f"register resources index {start_idx} to {last_idx - 1} ")

            # ... and register them in bulk
            resp = requests.post(
                f"{self.dcat_url}/datasets/register_resources", headers=request_headers, json=resource_defs
            )

            parsed_response = DCatAPI.handle_api_response(resp)
            resources = parsed_response["resources"]
            print(f"Registered dataset_id is: {resources[0]['dataset_id']}")

        return dataset, resources, variables

    def register_special_variables(
        self, variable_data: List
    ):
        if not variable_data:
            return []

        standard_variables = variable_data.get("standard_variables", [])
        variable_names = variable_data.get("variable_names", [])
        variable_metadata = variable_data.get("variable_metadata", [])
        if not variable_names:
            variable_names = [f"var_{idx}" for idx in range(len(variable_names))]
        if not variable_metadata:
            variable_metadata = [{} for idx in range(len(variable_names))]

        sv_forms = []

        for sv in standard_variables:
            sv_form = StandardVariableForm(sv["ontology"], sv["name"], sv["uri"])
            sv_forms.append(sv_form)

        ids = self.register_variables(sv_forms)
        return [{
            "name": variable_names[idx],
            "standard_name_id": [id],
            "metadata": variable_metadata[idx]
        } for idx, id in enumerate(ids)]

    def register_variables(self, variable_forms: List[StandardVariableForm]) -> List[str]:
        """
        Register variables to the data catalog.
        """
        DCatAPI.logger.debug("register variables: %s", variable_forms)
        request_headers = {"Content-Type": "application/json", "X-Api-Key": self.get_api_key()}
        resp = requests.post(
            f"{self.dcat_url}/knowledge_graph/register_standard_variables",
            headers=request_headers,
            json={"standard_variables": [var.__dict__ for var in variable_forms]},
        )

        parsed_resp = self.handle_api_response(resp)
        ids = []
        for var in variable_forms:
            for record in parsed_resp["standard_variables"]:
                if record["uri"] == var.uri:
                    ids.append(record["record_id"])
                    break
        return ids

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
