#!/usr/bin/python
# -*- coding: utf-8 -*-
import subprocess
from pathlib import Path
from typing import Union, Dict

from dcatreg.dcat_api import DCatAPI
from dtran.argtype import ArgType
from dtran.ifunc import IFunc


class GraphWriteFunc(IFunc):
    id = "graph_write_func"
    description = """ A writer adapter.
    Generates a csv/json file.
    """
    inputs = {
        "resource_path": ArgType.FilePath,
        "metadata": ArgType.OrderedDict,
        "provenance_id": ArgType.String,
        "name": ArgType.String,
    }
    outputs = {"data": ArgType.String}

    def __init__(self, resource_path: Union[str, Path], metadata: Dict, provenance_id: str, name: str):
        self.resource_path = Path(resource_path)
        self.metadata = metadata

        self.dcat = DCatAPI.get_instance()

        self.provenance_id = provenance_id
        self.name = name

    def exec(self) -> dict:
        response = "success"
        upload_output = subprocess.check_output(
            f"curl -sD - --user upload:HVmyqAPWDNuk5SmkLOK2 --upload-file {self.resource_path.absolute()} https://publisher.mint.isi.edu",
            shell=True,
        )

        upload_url = f'https://{upload_output.decode("utf-8").split("https://")[-1]}'

        self.metadata[0]["url"] = upload_url

        self.dcat.register_datasets(self.provenance_id, self.metadata)

        return {"response": response}

    def validate(self) -> bool:
        return True
