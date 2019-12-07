import argparse
import glob
import os
from multiprocessing.pool import Pool
from pathlib import Path

from drepr import NDArrayGraph, DRepr
from tqdm import tqdm

from funcs.gdal.raster import BoundingBox, Raster, ReSample, GeoTransform


def drepr2raster(drepr_file, infile):
    ndarray = NDArrayGraph.from_drepr(DRepr.parse_from_file(drepr_file), infile)
    class_id = list(ndarray.iter_class_ids("mint:Variable"))
    assert len(class_id) == 1
    class_id = class_id[0]
    class_info = ndarray.sm.class2dict(class_id)

    lat = ndarray.edge_data_as_ndarray(class_info["mint-geo:lat"], [])
    edge_data = ndarray.edge_data_as_ndarray(class_info['rdf:value'],
                                             [class_info['mint-geo:lat'], class_info['mint-geo:long']])

    if lat.data[0] < lat.data[1]:
        # create north-up images
        edge_data.data = edge_data.data[::-1]

    geo2d = ndarray._deprecated_get1rowtbl("mint-geo:Raster")
    gt = GeoTransform(x_min=geo2d['mint-geo:x_min'],
                      y_max=geo2d['mint-geo:y_min'] + geo2d['mint-geo:dy'] * edge_data.data.shape[0],
                      dx=geo2d['mint-geo:dx'], dy=-geo2d['mint-geo:dy'])

    raster = Raster(edge_data.data, gt, int(geo2d['mint-geo:epsg']),
                     edge_data.nodata.value if edge_data.nodata is not None else None)
    return raster


def crop_bounding_box(args, debug=False):
    drepr_file, infile, outfile, bounding_box = args
    raster = drepr2raster(drepr_file, infile)
    raster2 = raster.crop(bounding_box, resampling_algo=ReSample.BILINEAR)
    raster2.serialize(outfile)

    if debug:
        raster.to_geotiff(outfile + ".original.tif")
        raster2.to_geotiff(outfile + ".crop.tif")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--indir",
                        "-i",
                        default="/Users/rook/workspace/MINT/MINT-Transformation/data/gpm_monthly_2008_2017",
                        help="input dir")
    parser.add_argument("--outdir",
                        "-o",
                        default="/Users/rook/workspace/MINT/MINT-Transformation/data/gpm_monthly_2008_2017_ethiopia",
                        help="output dir")
    parser.add_argument("--model",
                        "-m",
                        default="/Users/rook/workspace/MINT/MINT-Transformation/examples/d3m/monthly_gpm.model.yml",
                        help="drepr model")
    parser.add_argument("--debug",
                        "-d",
                        action="store_true",
                        default=False,
                        help="enable debug")

    args = parser.parse_args()
    nc_file_lst = sorted(glob.glob(os.path.join(args.indir, "*.nc*")))
    ethiopia = BoundingBox(32.75418, 3.22206, 47.98942, 15.15943)
    # ethiopia = BoundingBox(-100.75418, -33.22206, 100.98942, 33.15943)

    fn_args = [
        (args.model, nc_file, os.path.join(args.outdir, f"{Path(nc_file).stem}.npz"), ethiopia)
        for nc_file in nc_file_lst
    ]

    if args.debug:
        crop_bounding_box(fn_args[0], args.debug)
    else:
        pool = Pool()
        for _ in tqdm(pool.imap_unordered(crop_bounding_box, fn_args), total=len(fn_args)):
            pass
