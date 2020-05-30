#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import datetime
from typing import Union, Generator, AsyncGenerator, Optional, Dict
from isodate import parse_duration
from dateutil import parser

from dtran.argtype import ArgType
from dtran.ifunc import IFunc, IFuncType
from dtran.dcat.api import DCatAPI
from dtran.metadata import Metadata


class DcatRangeStream(IFunc):
    id = "dcat_range_stream"
    description = """ Returns a stream of start_time and end_time for a dataset from Data Catalog
    """
    func_type = IFuncType.READER
    friendly_name: str = "Data Catalog Time Range Stream"
    inputs = {
        "dataset_id": ArgType.String,
        "start_time": ArgType.DateTime(optional=True),
        "end_time": ArgType.DateTime(optional=True),
        "step_time": ArgType.String(optional=True)
    }
    outputs = {
        "dataset_id": ArgType.String,
        "start_time": ArgType.DateTime,
        "end_time": ArgType.DateTime
    }
    example = {
        "dataset_id": "ea0e86f3-9470-4e7e-a581-df85b4a7075d",
        "start_time": "2020-03-02T12:30:55",
        "end_time": "2020-03-02T12:30:55",
        "step_time": "P3Y6M4DT12H30M5S",
    }

    def __init__(self, dataset_id: str, start_time: datetime = None, end_time: datetime = None, step_time: str = None):
        self.dataset_id = dataset_id
        if (start_time is None) or (end_time is None):
            dataset = DCatAPI.get_instance().find_dataset_by_id(dataset_id)
            self.start_time = start_time or parser.parse(dataset['metadata']['temporal_coverage']['start_time'])
            self.end_time = end_time or parser.parse(dataset['metadata']['temporal_coverage']['end_time'])
        else:
            self.start_time = start_time
            self.end_time = end_time

        self.start_time = self.start_time.replace(microsecond=0)
        self.end_time = self.end_time.replace(microsecond=0)
        if step_time is None:
            self.step_time = self.end_time - self.start_time
        else:
            self.step_time = parse_duration(step_time)

    async def exec(self) -> Union[dict, Generator[dict, None, None], AsyncGenerator[dict, None]]:
        start_time = self.start_time
        while start_time < self.end_time:
            end_time = min(start_time + self.step_time, self.end_time)
            yield {"dataset_id": self.dataset_id, "start_time": start_time, "end_time": end_time}
            start_time = end_time

    def validate(self) -> bool:
        return True

    def change_metadata(self, metadata: Optional[Dict[str, Metadata]]) -> Dict[str, Metadata]:
        return metadata
