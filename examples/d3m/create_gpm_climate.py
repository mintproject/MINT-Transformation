import argparse
import csv
import glob, numpy as np, fiona
import os
import re
import sys, gdal
from calendar import monthrange
from collections import OrderedDict
from multiprocessing.pool import Pool
from pathlib import Path

import ujson
from osgeo import ogr
from tqdm import tqdm

from crop_ethiopia import DATA_DIR
from funcs.gdal.raster import BoundingBox, Raster, ReSample


def get_woreda_name(shp_file):
    with fiona.open(shp_file, "r") as f:
        for line in f:
            return {"woreda_name": line['properties']['WOREDANAME'].strip(),
                    "zonename": line['properties']['ZONENAME'].strip(), "woreda_no": line['properties'][
                    'WOREDANO_']}


def compute_precipitation(args, debug=False):
    raster_files, shp_files, year, month = args
    assert len(raster_files) > 0

    data = []
    for raster_file in raster_files:
        raster = Raster.deserialize(raster_file)
        data.append(raster.data)
    data = np.stack(data, axis=0)
    raster = Raster(data, raster.geotransform, raster.epsg, raster.nodata)

    n_hours = monthrange(year, month)[1] * 24
    records = []

    for shp_file in shp_files:
        shp_name = Path(shp_file).name
        record = get_woreda_name(shp_file)

        try:
            cr = raster.crop(vector_file=shp_file, resampling_algo=ReSample.BILINEAR)
        except Exception as e:
            print(">>> invalid shape file", shp_file)
            raise

        if debug:
            cr.to_geotiff(str(DATA_DIR / "debug" / "gpm" / f"{shp_name}-{year}-{month}.tif"))
            print(">>>", 'shp=', cr.data.shape, '#nodata=', np.sum(cr.data == raster.nodata), '#data=',
                  np.sum(cr.data != raster.nodata),
                  '#size=', np.prod(cr.data.shape), '#gt0=', np.sum(cr.data < 0))
        n_valid_points = np.sum(cr.data != raster.nodata)
        assert n_valid_points == np.sum(cr.data >= 0), 'safety check'

        # do this because the unit is mm/hr
        precipitation = np.sum(cr.data[cr.data != raster.nodata]) * n_hours
        precipitation = precipitation / n_valid_points

        record['month'] = f"{year}-{month:02}-01"
        record['precipitation'] = precipitation
        records.append(record)
    return records


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--indir",
                        "-i",
                        default=str(DATA_DIR / "gpm_monthly_2008_2017_ethiopia"),
                        help="input dir")
    parser.add_argument('--date_regex', '-r',
                        default='3B-MO.MS.MRG.3IMERG.(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})',
                        help='regex that parse to get month and year')
    parser.add_argument("--shp_dir",
                        "-s",
                        default=str(DATA_DIR / "ethiopia-district-l3"),
                        help="shape dir")
    parser.add_argument("--outfile",
                        "-o",
                        default=str(DATA_DIR / "gpm_precipitation.csv"),
                        help="input dir")
    parser.add_argument("--debug",
                        "-d",
                        action="count",
                        default=0,
                        help="set debug level")

    args = parser.parse_args()
    date_regex = re.compile(args.date_regex)

    # group raster per year-month
    raster_files = sorted(glob.glob(os.path.join(args.indir, "*.npz")))
    grouped_rasters = OrderedDict()
    for fpath in raster_files:
        match = date_regex.match(Path(fpath).name)
        year = int(match.group('year'))
        month = int(match.group('month'))
        group_key = (year, month)
        if group_key not in grouped_rasters:
            grouped_rasters[group_key] = []
        grouped_rasters[group_key].append(fpath)

    shp_files = []
    for fpath in sorted(glob.glob(os.path.join(args.shp_dir, "woredas", "*.shp"))):
        fname = Path(fpath).name
        if os.path.exists(os.path.join(args.shp_dir, "woredas-fixed", fname)):
            shp_files.append(os.path.join(args.shp_dir, "woredas-fixed", fname))
        else:
            shp_files.append(fpath)

    if args.debug > 0:
        shp_files = shp_files[:args.debug]

    fn_args = [
        (files, shp_files, year, month)
        for (year, month), files in grouped_rasters.items()
    ]
    if args.debug > 0:
        records = []
        for aa in fn_args[:args.debug]:
            records += compute_precipitation(aa, True)
    else:
        pool = Pool()
        records = []
        for rr in tqdm(pool.imap_unordered(compute_precipitation, fn_args), total=len(fn_args)):
            records += rr

    # write output
    with open(args.outfile, "w") as f:
        writer = csv.writer(f)
        keys = sorted(records[0].keys())
        writer.writerow(keys)
        for r in records:
            writer.writerow([r[k] for k in keys])
