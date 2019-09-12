from pathlib import Path
from typing import Union

import xarray as xr

from dtran import IFunc, ArgType


class MintNetCDFWriteFunc(IFunc):
    id = "netcdf_write_func"
    inputs = {
        "data": ArgType.NDimArray,
        "output_file": ArgType.FilePath
    }

    outputs = {
        "result": ArgType.Boolean
    }

    def __init__(
        self,
        data: xr.Dataset,
        output_file: Union[str, Path],
    ):
        self.ndarray = data

        self.output_file = Path(output_file)

    def exec(self) -> dict:
        self.ndarray.to_netcdf(self.output_file)

        return {"result": True}

    def validate(self) -> bool:
        return True
