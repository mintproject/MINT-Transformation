#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import datetime
from typing import Union, Generator, AsyncGenerator, Optional, Dict

from dtran.argtype import ArgType
from dtran.ifunc import IFunc, IFuncType
from dtran.dcat.api import DCatAPI
from dtran.metadata import Metadata


class DcatVariableStream(IFunc):
    id = "dcat_variable_stream"
    description = """ Returns a stream of standard variables for a dataset from Data Catalog
    """
    func_type = IFuncType.READER
    friendly_name: str = "Data Catalog Standard Variable Stream"
    inputs = {
        "dataset_id": ArgType.String
    }
    outputs = {
        "variable_name": ArgType.String
    }
    example = {
        "dataset_id": "ea0e86f3-9470-4e7e-a581-df85b4a7075d"
    }

    def __init__(self, dataset_id: str):
        self.variables = DCatAPI.get_instance().find_standard_variables_by_dataset_id(dataset_id)

    async def exec(self) -> Union[dict, Generator[dict, None, None], AsyncGenerator[dict, None]]:
        for variable in self.variables:
            yield {"variable_name": variable["standard_variable_name"]}

    def validate(self) -> bool:
        return True

    def change_metadata(self, metadata: Optional[Dict[str, Metadata]]) -> Dict[str, Metadata]:
        return metadata
