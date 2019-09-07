from dtran import IFunc, ArgType


class NetCDFWriteFunc(IFunc):
    id = "netcdf_write_func"
    inputs = {
        "graph": ArgType.Graph(None),
        "latitude_attr": ArgType.String,
        "longitude_attr": ArgType.String,
        "elevation_attr": ArgType.String,
        "time_attr": ArgType.String
    }