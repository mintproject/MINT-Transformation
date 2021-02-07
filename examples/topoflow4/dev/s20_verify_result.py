import subprocess
import os
from pathlib import Path
from dateutil import parser
from datetime import timedelta, datetime
from dataclasses import dataclass
from tqdm.auto import tqdm
import numpy as np

HOME_DIR = Path(os.path.abspath(os.environ['HOME_DIR']))
DOWNLOAD_DIR = Path(os.path.abspath(os.environ['DATA_CATALOG_DOWNLOAD_DIR']))

# gldas is 8, gpm is 48
n_files_per_day = 48
run_dir = HOME_DIR / "data" / "tf_gpm"


def get_size(infile):
    data = np.fromfile(infile, dtype=np.float32)
    return data.shape[0]

n_month_days = [
    (parser.parse(f'2000-{i+1:02}-01T00:00') - parser.parse(f'2000-{i:02}-01T00:00')).days
    for i in range(1, 12)
]
n_month_days.append(31)

for year in range(2008, 2009):
    n_year_days = (parser.parse(f'{year+1}-01-01T00:00') - parser.parse(f'{year}-01-01T00:00')).days
    n_month_days[1] = (parser.parse(f'{year}-03-01T00:00') - parser.parse(f'{year}-02-01T00:00')).days

    for area_dir in (run_dir / str(year)).iterdir():
        area_name = area_dir.name
        if area_name.startswith("data_m"):
            continue
        print(f"check {year} area {area_name}")

        for arcsec in [30, 60]:
            with open(area_dir / f"output_r{arcsec}.rti", "r") as f:
                lines = f.readlines()
                n_cols = -1
                n_rows = -1
                for line in lines:
                    if line.startswith("Number of columns:"):
                        n_cols = int(line.replace("Number of columns:", "").strip())
                    if line.startswith("Number of rows:"):
                        n_rows = int(line.replace("Number of rows:", "").strip())
                assert n_cols != -1 and n_rows != -1

            infile = area_dir / f"output_r{arcsec}.rts"
            size = get_size(infile)
            assert size == n_rows * n_cols * n_files_per_day * n_year_days, f"Incorrect {infile}"

            for month in range(1, 13):
                infile = area_dir / f"output_r{arcsec}_m{month:02}.rts"
                size = get_size(infile)
                assert size == n_rows * n_cols * n_files_per_day * n_month_days[month-1]