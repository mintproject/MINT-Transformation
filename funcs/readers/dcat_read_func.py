#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import subprocess
import time
from datetime import datetime
from pathlib import Path

import requests
from dateutil import parser
from drepr import DRepr
from drepr.outputs import ArrayBackend, GraphBackend

from dtran.argtype import ArgType
from dtran.backend import SharedBackend
from dtran.ifunc import IFunc, IFuncType
from dtran.metadata import Metadata

DCAT_URL = "https://api.mint-data-catalog.org"


class DcatReadFunc(IFunc):
    id = "dcat_read_func"
    description = """ An entry point in the pipeline.
    Fetches a dataset and its metadata from the MINT Data-Catalog.
    """
    func_type = IFuncType.READER
    friendly_name: str = "Data Catalog Reader"
    inputs = {
        "dataset_id": ArgType.String,
        "start_time": ArgType.DateTime(optional=True),
        "end_time": ArgType.DateTime(optional=True),
        "use_cache": ArgType.Boolean(optional=True),
    }
    outputs = {"data": ArgType.DataSet(None)}
    example = {"dataset_id": "05c43c58-ed42-4830-9b1f-f01059c4b96f"}

    def __init__(
        self,
        dataset_id: str,
        start_time: datetime = None,
        end_time: datetime = None,
        use_cache: bool = True,
    ):
        # TODO: move to a diff arch (pointer to Data-Catalog URL)

        self.dataset_id = dataset_id
        self.logger = logging.getLogger(DcatReadFunc.id)

        dataset_result = DCatAPI.get_instance(DCAT_URL).find_dataset_by_id(dataset_id)

        assert ("resource_repr" in dataset_result["metadata"]) or (
            "dataset_repr" in dataset_result["metadata"]
        ), "Dataset is missing both 'resource_repr' and 'dataset_repr'"

        assert not (
            ("resource_repr" in dataset_result["metadata"])
            and ("dataset_repr" in dataset_result["metadata"])
        ), "Dataset has both 'resource_repr' and 'dataset_repr'"

        resource_results = DCatAPI.get_instance(DCAT_URL).find_resources_by_dataset_id(
            dataset_id
        )
        resource_ids = {}

        self.metadata = {}

        if "resource_repr" in dataset_result["metadata"]:
            self.repr = DRepr.parse(dataset_result["metadata"]["resource_repr"])
            for resource in resource_results:
                temporal_coverage = resource["resource_metadata"]["temporal_coverage"]

                if start_time or end_time:
                    temporal_coverage["start_time"] = parser.parse(
                        temporal_coverage["start_time"]
                    )
                    temporal_coverage["end_time"] = parser.parse(
                        temporal_coverage["end_time"]
                    )
                    if (start_time and start_time > temporal_coverage["end_time"]) or (
                        end_time and end_time < temporal_coverage["start_time"]
                    ):
                        continue

                spatial_coverage = resource["resource_metadata"]["spatial_coverage"][
                    "value"
                ]

                xmin = spatial_coverage["xmin"]
                ymin = spatial_coverage["ymin"]
                xmax = spatial_coverage["xmax"]
                ymax = spatial_coverage["ymax"]

                self.metadata[resource["resource_id"]] = Metadata(
                    start_time, end_time, xmin, ymin, xmax, ymax
                )

                resource_ids[resource["resource_id"]] = resource["resource_data_url"]
            self.repr_type = "resource_repr"
        else:
            # TODO: fix me!!
            assert len(resource_results) == 1
            resource_ids[resource_results[0]["resource_id"]] = resource_results[0][
                "resource_data_url"
            ]
            self.repr = DRepr.parse(dataset_result["metadata"]["dataset_repr"])
            self.repr_type = "dataset_repr"

        self.logger.debug(f"Found key '{self.repr_type}'")
        Path("/tmp/dcat_read_func").mkdir(exist_ok=True, parents=True)

        self.logger.debug(f"Downloading {len(resource_ids)} resources ...")
        self.resources = {}

        for resource_id, resource_url in resource_ids.items():
            file_full_path = f"/tmp/dcat_read_func/{resource_id}.dat"
            self.resources[resource_id] = file_full_path

            if use_cache and Path(file_full_path).exists():
                self.logger.debug(f"Skipping resource {resource_id}, found in cache")
                continue
            self.logger.debug(f"Downloading resource {resource_id} ...")

            subprocess.check_call(
                f"wget {resource_url} -O {file_full_path}", shell=True
            )

        self.logger.debug(f"Download Complete")

    def exec(self) -> dict:
        if (
            self.get_preference("data") is None
            or self.get_preference("data") == "array"
        ):
            backend = ArrayBackend
        else:
            backend = GraphBackend

        if self.repr_type == "dataset_repr":
            return {
                "data": backend.from_drepr(self.repr, list(self.resources.values())[0])
            }
        else:
            datasets = []
            for i, resource in enumerate(self.resources.values()):
                datasets.insert(0, backend.from_drepr(self.repr, resource))

            return {"data": SharedBackend(datasets)}

    def validate(self) -> bool:
        return True

    def change_metadata(self, metadata):
        return self.metadata
