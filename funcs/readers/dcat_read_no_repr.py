#!/usr/bin/python
# -*- coding: utf-8 -*-

import subprocess
from pathlib import Path
from typing import Dict, Optional

from dtran.argtype import ArgType
from dtran.ifunc import IFunc, IFuncType
from dtran.metadata import Metadata
from funcs.readers.dcat_read_func import DCatAPI, ResourceManager
import os

DATA_CATALOG_DOWNLOAD_DIR = os.path.abspath(os.environ["DATA_CATALOG_DOWNLOAD_DIR"])
if os.environ["NO_CHECK_CERTIFICATE"].lower().strip() == "true":
    DOWNLOAD_CMD = "wget --no-check-certificate"
else:
    DOWNLOAD_CMD = "wget"

Path(DATA_CATALOG_DOWNLOAD_DIR).mkdir(exist_ok=True, parents=True)


class DcatReadNoReprFunc(IFunc):
    id = "dcat_read_norepr_func"
    description = """ An entry point in the pipeline.
    Fetches a dataset and its metadata from the MINT Data-Catalog.
    """
    func_type = IFuncType.READER
    friendly_name: str = " Data Catalog Reader Without repr File"
    inputs = {"dataset_id": ArgType.String}
    outputs = {"data_path": ArgType.String}
    example = {"dataset_id": "05c43c58-ed42-4830-9b1f-f01059c4b96f"}

    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.resource = []

        resources = DCatAPI.get_instance().find_resources_by_dataset_id(dataset_id)

        self.resource_manager = ResourceManager.get_instance()

        assert len(resources) == 1

        self.resource_id = resources[0]["resource_id"]
        self.resource_metadata = {
            key: resources[0][key] for key in {"resource_data_url", "resource_type"}
        }

    def exec(self) -> dict:
        data_path = self.resource_manager.download(
            self.resource_id, self.resource_metadata, should_redownload=False
        )
        return {"data_path": data_path}

    def validate(self) -> bool:
        return True

    def change_metadata(
        self, metadata: Optional[Dict[str, Metadata]]
    ) -> Dict[str, Metadata]:
        return metadata
