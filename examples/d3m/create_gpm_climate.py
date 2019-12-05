import csv
import glob, numpy as np, fiona
import os
import re
import sys
from multiprocessing.pool import Pool
from pathlib import Path

from tqdm import tqdm

from funcs.gdal.raster import BoundingBox, Raster, ReSample

indir = sys.argv[1]
shp_dir = sys.argv[2]
outdir = sys.argv[3]
date_regex = re.compile('3B-HHR-E.MS.MRG.3IMERG.(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})')


def get_woreda_name(shp_file):
    with fiona.open(shp_file, "r") as f:
        for line in f:
            return line['properties']['WOREDANAME'].strip(), line['properties']['ZONENAME'].strip()


# concat all raster
raster_file_lst = sorted(glob.glob(os.path.join(indir, "*.npz")))
grid_files_per_month = {}
for grid_file in raster_file_lst:
    match = date_regex.match(Path(grid_file).name)
    month = f"{match.group('year')}-{match.group('month')}"
    if month not in grid_files_per_month:
        grid_files_per_month[month] = []
    grid_files_per_month[month].append(grid_file)

raster_per_months = {}
records = []
shp_files = sorted(glob.glob(os.path.join(shp_dir, "*.shp")))
for month in sorted(grid_files_per_month.keys()):
    grid_files = grid_files_per_month[month]
    data = []
    for raster_file in tqdm(grid_files, desc="concat raster " + month):
        raster = Raster.deserialize(raster_file)
        data.append(raster.data)
    data = np.stack(data, axis=0)
    raster = Raster(data, raster.geotransform, raster.epsg, raster.nodata)

    for shp_file in tqdm(shp_files, desc="cropping"):
        try:
            raster_woredas = raster.crop(vector_file=shp_file, resampling_algo=ReSample.BILINEAR)
            raster_woredas.data[np.where(raster_woredas.data < 0)] = 0.0
            total_prep = np.sum(raster_woredas.data)
            average_prep = np.mean(raster_woredas.data)
        except Exception as e:
            total_prep = 0.0
            average_prep = 0.0
        records.append([*get_woreda_name(shp_file), month + "-01", total_prep, average_prep])

with open(os.path.join(outdir, "output.csv"), "w") as f:
    writer = csv.writer(f)
    writer.writerow(['woredas', 'zone', 'month', 'total_precipitation', 'average_precipitation'])
    for record in records:
        writer.writerow(record)



