import datetime
from pathlib import Path
from typing import Union

import xarray as xr

from dtran import IFunc, ArgType


class MintNetCDFWriteFunc(IFunc):
    id = "netcdf_write_func"
    inputs = {
        "data": ArgType.NDimArray,
        "output_file": ArgType.FilePath,
        "title": ArgType.String,
        "comment": ArgType.String,
        "naming_authority": ArgType.String,
        "id": ArgType.String,
        "creator_name": ArgType.String,
        "creator_email": ArgType.String,
    }

    outputs = {
        "result": ArgType.Boolean
    }

    def __init__(
            self,
            data: xr.Dataset,
            output_file: Union[str, Path], title: str, comment: str, naming_authority: str, id: str, creator_name: str,
            creator_email: str
    ):
        self.ndarray = data
        self.output_file = Path(output_file)
        self.title = title
        self.comment = comment
        self.naming_authority = naming_authority
        self.id = id
        self.creator_name = creator_name
        self.creator_email = creator_email

    def exec(self) -> dict:
        x_min = min(self.ndarray.coords["X"])
        x_max = max(self.ndarray.coords["X"])
        y_min = min(self.ndarray.coords["Y"])
        y_max = max(self.ndarray.coords["Y"])
        self.ndarray.attrs.update({
            "title": self.title,
            "comment": self.comment,
            "naming_authority": self.naming_authority,
            "id": self.id,
            "date_created": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "date_modified": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "creator_name": self.creator_name,
            "geospatial_bounds_crs": "+init=epsg:4326",
            "geospatial_bounds": [x_min, y_min, x_max, y_max],
            "creator_email": self.creator_email}
        )
        self.ndarray.to_netcdf(self.output_file, format="NETCDF4")

        return {"result": True}

    def validate(self) -> bool:
        return True
