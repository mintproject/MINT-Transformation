#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging
import os
import subprocess
from collections import OrderedDict
from datetime import datetime
from pathlib import Path

from dateutil import parser
from drepr import DRepr
from drepr.outputs import ArrayBackend, GraphBackend

from dtran.argtype import ArgType
from dtran.backend import SharedBackend
from dtran.dcat.api import DCatAPI
from dtran.ifunc import IFunc, IFuncType
from dtran.metadata import Metadata

DCAT_URL = os.environ["DCAT_URL"]
DATA_CATALOG_DOWNLOAD_DIR = os.path.abspath(os.environ["DATA_CATALOG_DOWNLOAD_DIR"])
Path(DATA_CATALOG_DOWNLOAD_DIR).mkdir(exist_ok=True, parents=True)


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

    logger = logging.getLogger(__name__)

    def __init__(
        self,
        dataset_id: str,
        start_time: datetime = None,
        end_time: datetime = None,
        use_cache: bool = True,
    ):
        # TODO: move to a diff arch (pointer to Data-Catalog URL)

        self.dataset_id = dataset_id
        dataset_result = DCatAPI.get_instance(DCAT_URL).find_dataset_by_id(dataset_id)

        assert ("resource_repr" in dataset_result["metadata"]) or (
            "dataset_repr" in dataset_result["metadata"]
        ), "Dataset is missing both 'resource_repr' and 'dataset_repr'"
        assert not (
            ("resource_repr" in dataset_result["metadata"])
            and ("dataset_repr" in dataset_result["metadata"])
        ), "Dataset has both 'resource_repr' and 'dataset_repr'"

        resource_results = DCatAPI.get_instance(DCAT_URL).find_resources_by_dataset_id(
            dataset_id, start_time, end_time
        )
        # self.resource_results = resource_results
        # print(len(resource_results))
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

        if dataset_id == "ea0e86f3-9470-4e7e-a581-df85b4a7075d":
            self.repr = DRepr.parse_from_file(
                os.environ["HOME_DIR"] + "/examples/d3m/gpm.yml"
            )
            self.logger.info("Overwrite GPM")
        elif dataset_id == "5babae3f-c468-4e01-862e-8b201468e3b5":
            self.repr = DRepr.parse_from_file(
                os.environ["HOME_DIR"] + "/examples/d3m/gldas.yml"
            )
            self.logger.info("Overwrite GLDAS")

        self.logger.info(f"Found key '{self.repr_type}'")
        self.logger.info(f"Downloading {len(resource_ids)} resources ...")
        self.resources = OrderedDict()
        n_skip = 0
        n_download = 0
        for resource_id, resource_url in resource_ids.items():
            file_full_path = os.path.join(
                DATA_CATALOG_DOWNLOAD_DIR, f"{resource_id}.dat"
            )
            self.resources[resource_id] = file_full_path
            if use_cache and Path(file_full_path).exists():
                self.logger.debug(f"Skipping resource {resource_id}, found in cache")
                n_skip += 1
                continue
            self.logger.debug(f"Downloading resource {resource_id} ...")
            subprocess.check_call(
                f"wget {resource_url} -O {file_full_path}", shell=True
            )
            n_download += 1

        self.logger.info(
            f"Download Complete. Skip {n_skip} and download {n_download} resources"
        )

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
            dataset = SharedBackend(len(self.resources))
            for resource in self.resources.values():
                dataset.add(
                    backend.from_drepr(self.repr, resource, dataset.inject_class_id)
                )
            return {"data": dataset}

    def validate(self) -> bool:
        return True

    def change_metadata(self, metadata):
        return self.metadata
