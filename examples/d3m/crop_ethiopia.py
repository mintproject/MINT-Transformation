import glob
import os
import sys
from multiprocessing.pool import Pool
from pathlib import Path

from tqdm import tqdm

from funcs.gdal.raster import BoundingBox, Raster, ReSample

indir = sys.argv[1]
outdir = sys.argv[2]


def crop_ethiopia(args):
    infile, outfile, bounding_box = args
    raster = Raster.from_netcdf4(infile, "HQprecipitation")
    raster = raster.crop(bounding_box, resampling_algo=ReSample.BILINEAR)
    raster.serialize(outfile)
    # return raster
    # raster.to_geotiff(outfile)


nc_file_lst = sorted(glob.glob(os.path.join(indir, "*.nc*")))
ethiopia = BoundingBox(32.75418, 3.22206, 47.98942, 15.15943)
args = [
    (nc_file, os.path.join(outdir, f"{Path(nc_file).stem}.npz"), ethiopia)
    for nc_file in nc_file_lst
]

pool = Pool()

for _ in tqdm(pool.imap_unordered(crop_ethiopia, args), total=len(args)):
    pass
