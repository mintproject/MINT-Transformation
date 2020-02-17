#!/usr/bin/python
# -*- coding: utf-8 -*-

import subprocess
from pathlib import Path

from dtran.argtype import ArgType
from dtran.ifunc import IFunc, IFuncType
from funcs.readers.dcat_read_func import DCatAPI


class DcatReadNoReprFunc(IFunc):
    id = "dcat_read_norepr_func"
    description = """ An entry point in the pipeline.
    Fetches a dataset and its metadata from the MINT Data-Catalog.
    """
    func_type = IFuncType.READER
    friendly_name: str = " Data Catalog Reader Without repr File"
    inputs = {"dataset_id": ArgType.String}
    outputs = {"data": ArgType.String}

    def __init__(self, dataset_id: str):
        # TODO: move to a diff arch (pointer to Data-Catalog URL)
        DCAT_URL = "https://api.mint-data-catalog.org"

        self.dataset_id = dataset_id

        resource_results = DCatAPI.get_instance(DCAT_URL).find_resources_by_dataset_id(
            dataset_id
        )
        # TODO: fix me!!
        assert len(resource_results) == 1
        resource_ids = {"default": resource_results[0]["resource_data_url"]}
        Path("/tmp/dcat_read_func").mkdir(exist_ok=True, parents=True)

        self.resources = {}
        for resource_id, resource_url in resource_ids.items():
            file_full_path = f"/tmp/dcat_read_func/{resource_id}.dat"
            subprocess.check_call(
                f"wget {resource_url} -O {file_full_path}", shell=True
            )
            self.resources[resource_id] = file_full_path

    def exec(self) -> dict:
        input_dir_full_path = f"/data/{self.dataset_id}"
        for resource in self.resources.values():
            if not Path(input_dir_full_path).exists():
                print("Not exists")
                Path(input_dir_full_path).mkdir(parents=True)
            else:
                subprocess.check_output(f"rm -rf {input_dir_full_path}/*", shell=True)
            subprocess.check_call(
                f"tar -xvzf {resource} -C {input_dir_full_path}/", shell=True
            )
        return {"data": input_dir_full_path}

    def validate(self) -> bool:
        return True
