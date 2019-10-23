from typing import List, Dict, Tuple, Callable, Any, Optional
import xarray as xr, numpy as np

HQprecipitation = np.asarray([
    [58, 61, 81, 85],
    [22, 53, 74, 65],
    [36, 97, 46, 10],
    [61, 75, 39, 30],
], dtype=np.float32)
lon = np.asarray([-178.95, -178.85, -178.75, -178.65], dtype=np.float32)
lat = np.asarray([-88.95, -88.85, -88.75, -88.65], dtype=np.float32)

HQprecipitation = xr.DataArray(
    HQprecipitation,
    coords=[("lon", lon), ("lat", lat)],
    attrs={
        "title": "Surface Inundation",
        "standard_name": "land_water_surface__height_flood_index",
        "long_name": "Surface Inundation",
        "units": "m",
        "valid_min": 0.0,
        "valid_max": np.max(HQprecipitation),
        "missing_value": -999.0,
        "fill_value": -999.0,
    },
)

flood_dataset = xr.Dataset(
    data_vars={"HQprecipitation": HQprecipitation},
    attrs={
        "Grid.GridHeader": "BinMethod=ARITHMETIC_MEAN;\nRegistration=CENTER;\nLatitudeResolution=0.1;\nLongitudeResolution=0.1;\nNorthBoundingCoordinate=90;\nSouthBoundingCoordinate=-90;\nEastBoundingCoordinate=180;\nWestBoundingCoordinate=-180;\nOrigin=SOUTHWEST;\n",
        "Grid.fullnamepath": "/Grid",
    },
)

x_attrs = {
    "standard_name": "longitude",
    "long_name": "longitude",
    "axis": "X",
    "units": "degrees_east",
}
y_attrs = {
    "standard_name": "latitude",
    "long_name": "latitude",
    "axis": "Y",
    "units": "degrees_north",
}

flood_dataset.lon.attrs.update(x_attrs)
flood_dataset.lat.attrs.update(y_attrs)

flood_dataset.to_netcdf("/data/small_fake.nc4", format="NETCDF4")