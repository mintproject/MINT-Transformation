#!/usr/bin/python
# -*- coding: utf-8 -*-
import subprocess
from pathlib import Path
from typing import Union, Dict

import ujson as json

from dcatreg.dcat_api import DCatAPI
from dtran.argtype import ArgType
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
