from collections import OrderedDict
from pathlib import Path
from typing import List, Dict, Tuple, Callable, Any, Optional, Union

from drepr import outputs, DRepr
from drepr.outputs.base_output_sm import BaseOutputSM

from dtran import ArgType
from dtran.ifunc import IFunc, IFuncType
import xarray as xr, numpy as np


class NetCDFWriteFunc(IFunc):
    id = "netcdf4_write_func"
    description = """Write dataset to NetCDF4 format. Following CF 1.0 convention"""
    inputs = {
        "dataset": ArgType.DataSet(None),
        "output_file": ArgType.String,
        "output_drepr_file": ArgType.String(optional=True),
    }
    outputs = {}

    def __init__(self, dataset: BaseOutputSM, output_file: Union[str, Path],
                 output_drepr_file: Optional[Union[str, Path]] = None):
        self.dataset = dataset
        self.outpupt_file = output_file
        if output_drepr_file is None:
            # put the drepr file to the same folder of the output file with different
            # extension
            tmp = Path(output_file)
            if tmp.name.find(".") != -1:
                new_name = tmp.name[:tmp.name.rfind(".")] + ".yml"
            else:
                new_name = tmp.name + ".yml"
            self.output_drepr_file = str(tmp.parent / new_name)
        else:
            self.output_drepr_file = output_drepr_file

    def validate(self) -> bool:
        return True

    def exec(self) -> dict:
        mint = self.dataset.ns("https://mint.isi.edu/")
        mint_geo = self.dataset.ns("https://mint.isi.edu/geo")
        rdf = self.dataset.ns(outputs.Namespace.RDF)

        variables = {}
        for c in self.dataset.c(mint.Variable):
            # TODO: automatically discover properties and write them accordingly
            standard_name = c.p(mint.standardName).as_ndarray([]).data
            assert standard_name.size == 1
            standard_name = standard_name[0]

            if c.p(mint.timestamp) is not None:
                timestamp = c.p(mint.timestamp).as_ndarray([]).data
                assert timestamp.size == 1
                timestamp = timestamp[0]
            else:
                timestamp = None

            for raster_id, sc in c.group_by(mint_geo.raster):
                gt = self.dataset.get_record_by_id(raster_id)
                val = sc.p(rdf.value).as_ndarray([c.p(mint_geo.lat), c.p(mint_geo.long)])

                assert len(val.data.shape) == 2
                if timestamp is not None:
                    data = val.data.reshape(1, *val.data.shape)
                else:
                    data = val.data
                nodata = val.nodata.value
                if isinstance(nodata, np.number):
                    if isinstance(nodata, np.integer):
                        nodata = int(nodata)
                    else:
                        nodata = float(nodata)

                variables[f"var_{len(variables)}"] = {
                    "data": data,
                    "timestamp": np.asarray([timestamp]),
                    "lat": val.index_props[0],
                    "long": val.index_props[1],
                    "metadata": {
                        "standard_name": standard_name,
                        "_FillValue": nodata,
                        "missing_values": nodata,
                        "dx": gt.s('mint-geo:dx'),
                        "dy": gt.s("mint-geo:dy"),
                        "epsg": gt.s("mint-geo:epsg"),
                        "x_slope": gt.s("mint-geo:x_slope"),
                        "y_slope": gt.s("mint-geo:y_slope"),
                        "x_0": gt.s("mint-geo:x_0"),
                        "y_0": gt.s("mint-geo:y_0")
                    }
                }

        assert len(variables) > 0
        drepr = {
            "version": "2",
            "resources": "netcdf4",
            "attributes": {},
            "alignments": [],
            "semantic_model": {
                "prefixes": {
                    "mint": "https://mint.isi.edu/",
                    "mint-geo": "https://mint.isi.edu/geo"
                }
            }
        }

        # for xarray, if the coordinates (indexed dimensions) are different, they will
        # merge them, so we need figure out which coordinate we can re-use
        shared_dims = set()
        v0 = next(iter(variables.values()))
        for dim in ['timestamp', 'lat', 'long']:
            if all(np.array_equal(v[dim], v0[dim]) for v in variables.values()):
                shared_dims.add(dim)

        # create netcdf variables
        xr_vars = {}
        for vid, var in variables.items():
            # the order of coords is very important
            coords = OrderedDict()
            for dim in (['timestamp', 'lat', 'long'] if var['timestamp'][0] is not None else ['lat', 'long']):
                if dim in shared_dims:
                    coords[dim] = var[dim]
                else:
                    coords[f'{vid}_{dim}'] = var[dim]

            xr_var = xr.DataArray(var['data'], dims=list(coords.keys()), coords=coords)
            for k, v in var['metadata'].items():
                xr_var.attrs[k] = v
            xr_vars[vid] = xr_var

            # update d-repr model
            drepr['attributes'][vid] = {
                "path": f"$.{vid}.data[0][:][:]" if var['timestamp'][0] is not None else f"$.{vid}.data[:][:]",
                "missing_values": [var['metadata']['missing_values']]
            }
            for gt_k in ["dx", "dy", "epsg", "x_slope", "y_slope", "x_0", "y_0"]:
                drepr['attributes'][f"{vid}_{gt_k}"] = f"$.{vid}.@.{gt_k}"
                drepr['alignments'].append({
                    "type": "dimension",
                    "source": vid,
                    "target": f"{vid}_{gt_k}",
                    "aligned_dims": []
                })

            drepr_sm_class = {
                "properties": [
                    ("rdf:value", vid),
                ],
                "links": [
                    ("mint-geo:raster", f"mint-geo:Raster:{len(xr_vars)}")
                ]
            }
            drepr['semantic_model'][f"mint:Variable:{len(xr_vars)}"] = drepr_sm_class
            drepr['semantic_model'][f"mint-geo:Raster:{len(xr_vars)}"] = {
                "properties": [
                    (f"mint-geo:{gt_k}", f"{vid}_{gt_k}")
                    for gt_k in ["dx", "dy", "epsg", "x_slope", "y_slope", "x_0", "y_0"]
                ]
            }

            # need order of coords match with (timestamp, lat, long)
            for coord_idx, (cid, coord) in enumerate(coords.items()):
                if cid not in drepr['attributes']:
                    if cid.endswith("timestamp"):
                        drepr['attributes'][cid] = f"$.{cid}.data[0]"
                    else:
                        drepr['attributes'][cid] = f"$.{cid}.data[:]"

                if cid.endswith("timestamp"):
                    drepr['alignments'].append({
                        "type": "dimension",
                        "source": vid,
                        "target": cid,
                        "aligned_dims": []
                    })
                    drepr_sm_class['properties'].append(("mint:timestamp", cid))
                else:
                    drepr['alignments'].append({
                        "type": "dimension",
                        "value": f"{vid}:{coord_idx + 2} <-> {cid}:2",
                    })
                    if cid.endswith("lat"):
                        drepr_sm_class['properties'].append(("mint-geo:lat", cid))
                    else:
                        drepr_sm_class['properties'].append(("mint-geo:long", cid))

        with open(self.output_drepr_file, 'w') as f:
            f.write(DRepr.parse(drepr).to_lang_yml(use_json_path=True))

        ds = xr.Dataset(xr_vars)
        ds.attrs.update({
            "conventions": "CF-1.6",
        })
        ds.to_netcdf(self.outpupt_file)
        return {}
