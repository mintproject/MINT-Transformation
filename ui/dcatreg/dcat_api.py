# !/usr/bin/env python
# coding: utf-8
import logging, time
from typing import List, Dict

import requests
import datetime

DATA_CATALOG_API = "https://api.mint-data-catalog.org"


class StandardVariableForm:
    def __init__(self, ontology: str, name: str, uri: str):
        self.ontology = ontology
        self.name = name
        self.uri = uri


class DCatAPI:
    instance = None
    logger = logging.getLogger("dcat_api")
    logger_api_resp = logging.getLogger("dcat_api.handle_response")

    def __init__(self):
        self.api_key = None

    @staticmethod
    def get_instance():
        if DCatAPI.instance is None:
            DCatAPI.instance = DCatAPI()
        return DCatAPI.instance

    def register_dataset_with_multiple_resources(
        self,
        provenance_id: str,
        name: str,
        description: str,
        variables: List[Dict[str, str]],
        resources_metadata: List[Dict],
        record_id=None,
    ):
        request_headers = {"Content-Type": "application/json", "X-Api-Key": self.get_api_key()}

        # Registering a dataset
        dataset = {"provenance_id": provenance_id, "name": name, "description": description, "metadata": {}}
        if record_id is not None:
            dataset["record_id"] = record_id
        resp = requests.post(
            f"{DATA_CATALOG_API}/datasets/register_datasets",
            headers=request_headers,
            json={"datasets": [dataset]},
        )

        parsed_response = DCatAPI.handle_api_response(resp)
        dataset["record_id"] = parsed_response["datasets"][0]["record_id"]

        # register variables
        resp = requests.post(
            f"{DATA_CATALOG_API}/datasets/register_variables",
            headers={"Content-Type": "application/json", "X-Api-Key": self.get_api_key()},
            json={
                "variables": [
                    {
                        "dataset_id": dataset["record_id"],
                        "name": var["name"],
                        "metadata": {},
                        "standard_variable_ids": var["standard_name_id"],
                    }
                    for var in variables
                ]
            },
        )

        variables = DCatAPI.handle_api_response(resp)["variables"]

        # Getting the bounding box of spatial coverage
        # get_boundingbox_country_output = self.get_boundingbox_country(spatial_coverage)
        resources = resources_metadata
        variable_records_ids = [v["record_id"] for v in variables]

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

        self.logger.debug("register resources: %s", resources)

        # ... and register them in bulk
        resp = requests.post(
            f"{DATA_CATALOG_API}/datasets/register_resources", headers=request_headers, json=resource_defs
        )

        parsed_response = DCatAPI.handle_api_response(resp)

        resources = parsed_response["resources"]
        return dataset, resources, variables

    def delete_resources(self, provenance_id, resource_ids):
        request_headers = {"Content-Type": "application/json", "X-Api-Key": self.get_api_key()}

        deleted_ids = []
        # Walking through each resource in dataset_obj
        for resource_id in resource_ids:
            resource_defs = {
                "provenance_id": provenance_id,
                "resource_id": resource_id,
            }

            resp = requests.post(
                f"{DATA_CATALOG_API}/resources/delete_resource", headers=request_headers, json=resource_defs
            )

            parsed_response = DCatAPI.handle_api_response(resp)

            print(parsed_response)

        return deleted_ids

    def register_dataset(
        self,
        provenance_id: str,
        name: str,
        description: str,
        variables: List[Dict[str, str]],
        url: str,
        filetype: str,
        start_time: str,
        end_time: str,
        spatial_coverage: List[float],
        record_id=None,
    ):
        request_headers = {"Content-Type": "application/json", "X-Api-Key": self.get_api_key()}

        # Registering a dataset
        dataset = {"provenance_id": provenance_id, "name": name, "description": description, "metadata": {}}
        if record_id is not None:
            dataset["record_id"] = record_id
        resp = requests.post(
            f"{DATA_CATALOG_API}/datasets/register_datasets",
            headers=request_headers,
            json={"datasets": [dataset]},
        )

        parsed_response = DCatAPI.handle_api_response(resp)
        dataset["record_id"] = parsed_response["datasets"][0]["record_id"]

        # register variables
        resp = requests.post(
            f"{DATA_CATALOG_API}/datasets/register_variables",
            headers={"Content-Type": "application/json", "X-Api-Key": self.get_api_key()},
            json={
                "variables": [
                    {
                        "dataset_id": dataset["record_id"],
                        "name": var["name"],
                        "metadata": {},
                        "standard_variable_ids": var["standard_name_id"],
                    }
                    for var in variables
                ]
            },
        )

        variables = DCatAPI.handle_api_response(resp)["variables"]
        variable_records_ids = [v["record_id"] for v in variables]


        # Getting the bounding box of spatial coverage
        # get_boundingbox_country_output = self.get_boundingbox_country(spatial_coverage)
        resources = [
            {
                "name": name.strip() + "." + filetype.lower(),
                "data_url": url,
                "resource_type": filetype.lower(),
                "metadata": {
                    "spatial_coverage": {
                        "type": "BoundingBox",
                        "value": {
                            "xmin": spatial_coverage[0],
                            "ymin": spatial_coverage[1],
                            "xmax": spatial_coverage[2],
                            "ymax": spatial_coverage[3],
                        },
                    },
                    "temporal_coverage": {"start_time": start_time, "end_time": end_time},
                },
            }
        ]

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

        self.logger.debug("register resources: %s", resources)

        # ... and register them in bulk
        resp = requests.post(
            f"{DATA_CATALOG_API}/datasets/register_resources", headers=request_headers, json=resource_defs
        )

        parsed_response = DCatAPI.handle_api_response(resp)

        resources = parsed_response["resources"]
        return dataset, resources, variables

    def register_variables(self, variable_forms: List[StandardVariableForm]) -> List[str]:
        """
        Register variables to the data catalog.
        """
        DCatAPI.logger.debug("register variables: %s", variable_forms)
        request_headers = {"Content-Type": "application/json", "X-Api-Key": self.get_api_key()}
        resp = requests.post(
            f"{DATA_CATALOG_API}/knowledge_graph/register_standard_variables",
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

    def find_dataset_by_id(self, dataset_id: str):
        request_headers = {"Content-Type": "application/json", "X-Api-Key": self.get_api_key()}
        resp = requests.post(
            f"{DATA_CATALOG_API}/datasets/find",
            headers=request_headers,
            json={"dataset_ids__in": [dataset_id]},
        ).json()
        assert resp["result"] == "success", resp["result"]
        return resp["resources"]

    def update_layout(self, dataset_id: str, layout: dict):
        request_headers = {"Content-Type": "application/json", "X-Api-Key": self.get_api_key()}
        resp = requests.post(
            f"{DATA_CATALOG_API}/datasets/update_dataset",
            headers=request_headers,
            json={"dataset_id": dataset_id, "metadata": {"layout": layout}},
        ).json()
        assert resp["success"], resp

    def standard_variable_search(self, standard_name_uri: str) -> List[str]:
        """
        Searching for standard variable currently present in the data catalog.
        :param standard_name_uri:
        :return: id of standard variable currently searching.
        """
        # Getting the Request Header
        request_headers = {"Content-Type": "application/json", "X-Api-Key": self.get_api_key()}

        search_query = {"uri__in": [standard_name_uri]}

        resp = requests.post(
            f"{DATA_CATALOG_API}/knowledge_graph/find_standard_variables",
            headers=request_headers,
            json=search_query,
        )
        parsed_response = DCatAPI.handle_api_response(resp)

        DCatAPI.logger.debug("search for variable: `%s`. Result: `%s`", standard_name_uri, parsed_response)

        return [x["id"] for x in parsed_response["standard_variables"]]

    def get_api_key(self):
        """
        :return:
        """
        if self.api_key is None or time.time() - self.api_key["time"] > 600:
            # Obtaining the API Key which allows us to make posts
            resp = requests.get(f"{DATA_CATALOG_API}/get_session_token").json()
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

    @staticmethod
    def get_boundingbox_country(country, output_as="boundingbox"):
        """
        get the bounding box of a country in EPSG4326 given a country name

        Parameters
        ----------
        country : str
            name of the country in english and lowercase
        output_as : 'str
            chose from 'boundingbox' or 'center'.
             - 'boundingbox' for [latmin, latmax, lonmin, lonmax]
             - 'center' for [latcenter, loncenter]

        Returns
        -------
        output : list
            list with coordinates as str
        """
        # create url
        url = "{0}{1}{2}".format(
            "http://nominatim.openstreetmap.org/search?country=", country, "&format=json&polygon=0"
        )

        response = requests.get(url).json()[0]

        # parse response to list
        if output_as == "boundingbox":
            lst = response[output_as]
            output = [float(i) for i in lst]
        if output_as == "center":
            lst = [response.get(key) for key in ["lat", "lon"]]
            output = [float(i) for i in lst]
        return output


if __name__ == "__main__":
    pass
