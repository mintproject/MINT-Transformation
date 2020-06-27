import subprocess
import os
from pathlib import Path
from dateutil import parser
from datetime import timedelta, datetime
from dataclasses import dataclass
from tqdm.auto import tqdm


HOME_DIR = Path(os.path.abspath(os.environ['HOME_DIR']))
DOWNLOAD_DIR = Path(os.path.abspath(os.environ['DATA_CATALOG_DOWNLOAD_DIR']))

arcsecs = [30, 60]
areas = {
    "awash": "37.829583333333, 6.654583333333, 39.934583333333, 9.374583333333",
    "baro": "34.221249999999, 7.362083333332, 36.450416666666, 9.503749999999",
    "shebelle": "38.159583333333, 6.319583333333, 43.559583333333, 9.899583333333",
    "ganale": "39.174583333333, 5.527916666666, 41.124583333333, 7.098749999999",
    "guder": "37.149583333333, 8.596250000000, 38.266250000000, 9.904583333333",
    "muger": "37.807916666667, 8.929583333333, 39.032916666667, 10.112916666667",
    "beko": "35.241249999999, 6.967916666666, 35.992916666666, 7.502916666666",
    "alwero": "34.206249999999, 7.415416666666, 35.249583333333, 8.143749999999"
}

run_dir = HOME_DIR / "data" / "tf_gldas"
commands = []

def add_geotif_command(commands, start_time, end_time, geotiff_dir):
    writegeotiff_config_file = str(HOME_DIR / "examples" / "topoflow4" / "dev" / "tf_climate_writegeotiff.gldas_remote.yml")
    cmd = f"""dotenv run python -m dtran.main exec_pipeline --config {writegeotiff_config_file} \
    --dataset.start_time={start_time} \
    --dataset.end_time={end_time} \
    --geotiff_writer.output_dir={geotiff_dir} \
    --geotiff_writer.skip_on_exist=true \
            """.strip()
    commands.append(cmd)


def add_rts_command(commands, start_time, end_time, geotiff_dir, geotiff_dir_crop, output_file, arcsec, area):
    makerts_config_file = str(HOME_DIR / "examples" / "topoflow4" / "dev" / "tf_climate_make_rts.yml")
    cmd = f"""dotenv run python -m dtran.main exec_pipeline --config {makerts_config_file} \
    --topoflow.geotiff_files='{geotiff_dir}*/*.tif' \
    --topoflow.cropped_geotiff_dir={geotiff_dir_crop} \
    --topoflow.output_file={output_file} \
    --topoflow.skip_crop_on_exist=true \
    --topoflow.xres_arcsecs={arcsec} \
    --topoflow.yres_arcsecs={arcsec} \
    --topoflow.bounds="{area}"
            """.strip()
    commands.append(cmd)


for year in range(2016, 2020):
    for month in range(1, 13):
        s0 = parser.parse(f"{year}-{month:02d}-01T00:00:00")
        if month == 12:
            s1 = s0.replace(day=31, hour=23, minute=59, second=59)
        else:
            s1 = s0.replace(month=s0.month + 1) - timedelta(seconds=1)

        start_time = s0.isoformat()
        end_time = s1.isoformat()
        geotiff_dir = str(run_dir / str(year) / f"data_m{month:02d}")
        add_geotif_command(commands, start_time, end_time, geotiff_dir)

        for area_name, area in areas.items():
            for arcsec in arcsecs:
                geotiff_dir_crop = str(run_dir / str(year) / area_name / f"geotiff_crop_{arcsec}")
                output_file = str(run_dir / str(year) / area_name / f"output_r{arcsec}_m{month:02d}.rts")
                add_rts_command(commands, start_time, end_time, geotiff_dir, geotiff_dir_crop, output_file, arcsec, area)

    start_time = f"{year}-01-01T00:00:00"
    end_time = f"{year}-12-31T23:59:59"
    geotiff_dir = str(run_dir / str(year) / f"data_m")

    for area_name, area in areas.items():
        for arcsec in arcsecs:
            geotiff_dir_crop = str(run_dir / str(year) / area_name / f"geotiff_crop_{arcsec}")
            output_file = str(run_dir / str(year) / area_name / f"output_r{arcsec}.rts")
            add_rts_command(commands, start_time, end_time, geotiff_dir, geotiff_dir_crop, output_file, arcsec, area)


commands = commands[:]
for cmd in tqdm(commands, desc="run commands"):
    output = subprocess.check_output(cmd, shell=True, cwd=str(HOME_DIR))