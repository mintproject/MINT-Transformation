import csv
import glob, numpy as np, fiona
import os
import re
import sys
from pathlib import Path
from calendar import monthrange
from tqdm import tqdm
from multiprocessing.pool import Pool
from funcs.gdal.raster import EPSG, Raster, GeoTransform, ReSample
from netCDF4 import Dataset
import gdal

infile = sys.argv[1]
shp_dir = sys.argv[2]
outfile = sys.argv[3]

# infile = "/Users/rook/workspace/MINT/MINT-Transformation/data/flood.nc"
# shp_dir = "/Users/rook/workspace/MINT/MINT-Transformation/data/woredas"
# outfile = "/Users/rook/workspace/MINT/MINT-Transformation/data/flood.csv"

year = int(sys.argv[4])


def get_woreda_details(shp_file):
    with fiona.open(shp_file, "r") as f:
        for line in f:
            return line['properties']['WOREDANAME'].strip(), line['properties']['ZONENAME'].strip(), line['properties'][
                    'WOREDANO_']


def process_shapefile(shp_file):
    global raster
    try:
        raster_woredas = raster.crop(vector_file=shp_file)
    except Exception as e:
        print(">>> error", shp_file)
        return [[*get_woreda_details(shp_file), f"{year}-{'%02d' % month}-01", 0, 0, 0] for month in range(1, 13)]
    record_list = []
    index = 0
    for month in range(1, 13):
        month_counts = [0, 0, 0, 0]
        _, days = monthrange(year, month)
        for day in range(index, index + days):
            flood_stat = int(np.max(raster_woredas.data[day]))
            assert 0 <= flood_stat < 4
            if flood_stat > 0:
                for j in range(flood_stat, 0, -1):
                    month_counts[j] += 1

            # for flood_stat in np.unique(raster_woredas.data[day]):
            #     flood_stat = int(flood_stat)
            #     if flood_stat > 0:
            #         # for j in range(len(month_counts) - 1, flood_stat - 1, -1):
            #         # flood_stat = class: 0, 1, 2, 3
            #         for j in range(flood_stat, 0, -1):
            #             month_counts[j] += 1
        index += days
        record_list.append([*get_woreda_details(shp_file), f"{year}-{'%02d' % month}-01", *month_counts[1:]])
    return record_list

shp_files = []
for fpath in sorted(glob.glob(os.path.join(shp_dir, "woredas", "*.shp"))):
    fname = Path(fpath).name
    if os.path.exists(os.path.join(shp_dir, "woredas-fixed", fname)):
        shp_files.append(os.path.join(shp_dir, "woredas-fixed", fname))
    else:
        shp_files.append(fpath)

ds = Dataset(infile)
gdal_ds = gdal.Open("NETCDF:{0}:{1}".format(infile, 'flood'), gdal.GA_ReadOnly)
raster = Raster(np.nan_to_num(np.asarray(ds.variables['flood']), nan=0.0), GeoTransform.from_gdal(gdal_ds.GetGeoTransform()), EPSG.WGS_84)

if __name__ == '__main__':
    records = []
    pool = Pool()
    # for record_list in tqdm(pool.imap_unordered(process_shapefile, shp_files), total=len(shp_files)):
    for shp_file in tqdm(shp_files):
        record_list = process_shapefile(shp_file)
        records.extend(record_list)
    with open(outfile, "w") as f:
        writer = csv.writer(f)
        writer.writerow(['woreda', 'zone', 'woreda_no', 'month', '2-yr_flooding', '5-yr_flooding', '20-yr_flooding'])
        for record in records:
            writer.writerow(record)
