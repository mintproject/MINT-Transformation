#!/usr/bin/python
# -*- coding: utf-8 -*-
import subprocess
from pathlib import Path
from typing import Union, Dict

from dcatreg.dcat_api import DCatAPI
from dtran.argtype import ArgType
from dtran.ifunc import IFunc


class DcatWriteFunc(IFunc):
    id = "dcat_write_func"
    description = """ A writer adapter.
    Write files to DCAT.
    """
    inputs = {
        "resource_path": ArgType.String,
        "metadata": ArgType.OrderedDict,
    }
    outputs = {"data": ArgType.String}

    PROVENANCE_ID = "b3e79dc2-8fa1-4203-ac82-b5267925191f"

    def __init__(
        self, resource_path: Union[str, Path], metadata: Dict,
    ):
        self.resource_path = Path(resource_path)
        self.metadata = metadata

        self.dcat = DCatAPI.get_instance()

    def exec(self) -> dict:
        response = "success"
        upload_output = subprocess.check_output(
            f"curl -sD - --user upload:HVmyqAPWDNuk5SmkLOK2 --upload-file {self.resource_path.absolute()} https://publisher.mint.isi.edu",
            shell=True,
        )

        upload_url = f'https://{upload_output.decode("utf-8").split("https://")[-1]}'

        self.metadata[0]["url"] = upload_url

        self.dcat.register_datasets(self.PROVENANCE_ID, self.metadata)

        return {"response": response}

    def validate(self) -> bool:
        return True
