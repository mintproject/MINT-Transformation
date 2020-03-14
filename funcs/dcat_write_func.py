#!/usr/bin/python
# -*- coding: utf-8 -*-
import subprocess
from pathlib import Path
from typing import Union
import os

import ujson as json

from dtran.argtype import ArgType
from dtran.dcat.api import DCatAPI
from dtran.ifunc import IFunc, IFuncType


class DcatWriteFunc(IFunc):
    id = "dcat_write_func"
    description = """ A writer adapter.
    Write files to DCAT.
    """
    func_type = IFuncType.WRITER
    inputs = {
        "resource_path": ArgType.String,
        "metadata": ArgType.String,
    }
    outputs = {"data": ArgType.String}
    friendly_name: str = "Data Catalog Writer"
    func_type = IFuncType.WRITER
    example = {
        "resource_path": "$.my_graph_write_func.output_file",
        "metadata": '[{"name": "WFP Food Prices - South Sudan", "description": "Food price dataset for South Sudan (2012-2019)"}]'
    }

    PROVENANCE_ID = "b3e79dc2-8fa1-4203-ac82-b5267925191f"

    def __init__(
        self, resource_path: Union[str, Path], metadata: str,
    ):
        self.resource_path = Path(resource_path)
        print(metadata)
        self.metadata = json.loads(metadata)

        self.dcat = DCatAPI.get_instance()

    def exec(self) -> dict:
        upload_output = subprocess.check_output(
            f"curl -sD - --user upload:HVmyqAPWDNuk5SmkLOK2 --upload-file {self.resource_path.absolute()} https://publisher.mint.isi.edu",
            shell=True,
        )

        upload_url = f'https://{upload_output.decode("utf-8").split("https://")[-1]}'

        self.metadata[0]["url"] = upload_url

        response = self.dcat.register_datasets(self.PROVENANCE_ID, self.metadata)

        print(response)
        return {"data": response}

    def validate(self) -> bool:
        return True


class DcatWriteMetadataFunc(IFunc):
    id = "dcat_write_metadata_func"
    description = """ A data catalog metadata writer adapter.
    """
    func_type = IFuncType.WRITER
    inputs = {
        "metadata": ArgType.String,
        "dataset_id": ArgType.String
    }
    outputs = {"data": ArgType.String}
    friendly_name: str = "Data Catalog Metadata Writer"
    example = {
        "metadata": '[{"name": "WFP Food Prices - South Sudan", "description": "Food price dataset for South Sudan (2012-2019)"}]',
        "dataset_id": "ea0e86f3-9470-4e7e-a581-df85b4a7075d"
    }

    PROVENANCE_ID = "b3e79dc2-8fa1-4203-ac82-b5267925191f"

    def __init__(
        self, metadata: str, dataset_id: str
    ):
        self.metadata = json.loads(metadata)
        self.dataset_id = dataset_id
        self.dcat = DCatAPI.get_instance()

    def exec(self) -> dict:
        response = self.dcat.update_dataset_metadata(
            self.PROVENANCE_ID, self.dataset_id, self.metadata
        )
        return {"data": response}

    def validate(self) -> bool:
        return True


class DcatBatchWriteFunc(IFunc):
    id = "dcat_batch_write_func"
    description = """ A batch writer adapter.
    Register a DCAT dataset with multiple resources.
    """
    func_type = IFuncType.WRITER
    inputs = {
        "resource_json_path": ArgType.String,
        "metadata": ArgType.String,
        "name": ArgType.String,
        "description": ArgType.String,
        "resource_type": ArgType.String,
        "variable_json_path": ArgType.String
    }
    outputs = {"data": ArgType.String}
    friendly_name: str = "Data Catalog Batch Writer"
    func_type = IFuncType.WRITER
    example = {
        "resource_json_path": "./uploaded.json",
        "metadata": '[{"name": "WFP Food Prices - South Sudan", "description": "Food price dataset for South Sudan (2012-2019)"}]',
        "name": "Test DCAT Dataset",
        "description": "Test description for test dataset",
        "resource_type": "zip",
        "variable_json_path": "./variable.json"
    }

    PROVENANCE_ID = "b3e79dc2-8fa1-4203-ac82-b5267925191f"

    def __init__(
        self, resource_json_path: Union[str, Path], metadata: str, name: str,
            description: str, resource_type: str, variable_json_path: Union[str, Path]
    ):
        if not os.path.exists(resource_json_path) or "json" not in str(resource_json_path) or "json" not in str(variable_json_path):
            raise ValueError(f"Please enter a valid json file path! Your input: {resource_json_path}, {variable_json_path}")
        with open(resource_json_path, "r") as f1, open(variable_json_path, "r") as f2:
            self.resource_data = json.load(f1)
            self.variable_data = json.load(f2)
        self.metadata = json.loads(metadata)
        self.name = name
        self.description = description
        self.resource_type = resource_type
        self.dcat = DCatAPI.get_instance()

    def exec(self) -> dict:
        # register variables
        variables = self.dcat.register_special_variables(self.variable_data)

        # create resource metadata
        resources_metadata = [
            {
                "data_url": file_url,
                "name": file_name,
                "resource_type": self.resource_type.lower(),
                "metadata": {},
            }
            for file_name, file_url in enumerate(self.resource_data)
        ]

        response = self.dcat.register_dataset_with_multiple_resources(
            self.PROVENANCE_ID,
            self.name,
            self.description,
            variables,
            resources_metadata,
            dataset_metadata=self.metadata,
        )
        return {"data": response}

    def validate(self) -> bool:
        return True
