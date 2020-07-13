import subprocess
import os
from pathlib import Path
from dateutil import parser
from datetime import timedelta, datetime
from dataclasses import dataclass
from tqdm.auto import tqdm


HOME_DIR = Path(os.path.abspath(os.environ['HOME_DIR']))
DOWNLOAD_DIR = HOME_DIR / "data/GPM"

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

run_dir = HOME_DIR / "data" / "tf_gpm"

def add_geotif_command(commands, year, month, geotiff_dir):
    writegeotiff_config_file = str(HOME_DIR / "examples" / "topoflow4" / "dev" / "tf_climate_writegeotiff.gpm.yml")
    resource_path = str(DOWNLOAD_DIR / str(year) / f"*3IMERG.{year}{month:02d}*")
    gpm_file = str(HOME_DIR / "examples/topoflow4/dev/gpm.yml")

    cmd = f"""dotenv run python -m dtran.main exec_pipeline --config {writegeotiff_config_file} \
    --dataset.resource_path={resource_path} \
    --dataset.repr_file={gpm_file} \
    --geotiff_writer.output_dir={geotiff_dir} \
    --geotiff_writer.skip_on_exist=true \
            """.strip()
    commands.append(cmd)


def add_rts_command(commands, geotiff_dir, geotiff_dir_crop, output_file, arcsec, area):
    makerts_config_file = str(HOME_DIR / "examples" / "topoflow4" / "dev" / "tf_climate_make_rts.yml")
    cmd = f"""dotenv run python -m dtran.main exec_pipeline --config {makerts_config_file} \
    --topoflow.geotiff_files='{geotiff_dir}*/*.tif' \
    --topoflow.cropped_geotiff_dir={geotiff_dir_crop} \
    --topoflow.output_file={output_file} \
    --topoflow.skip_crop_on_exist=true \
    --topoflow.xres_arcsecs={arcsec} \
    --topoflow.yres_arcsecs={arcsec} \
    --topoflow.bounds="{area}" \
    --topoflow.unit_multiplier=1
            """.strip()
    commands.append(cmd)


for year in range(2010, 2020):
    commands = []
    (run_dir / str(year)).mkdir(exist_ok=True, parents=True)
    for month in range(1, 13):
        s0 = parser.parse(f"{year}-{month:02d}-01T00:00:00")
        if month == 12:
            s1 = s0.replace(day=31, hour=23, minute=59, second=59)
        else:
            s1 = s0.replace(month=s0.month + 1) - timedelta(seconds=1)

        start_time = s0.isoformat()
        end_time = s1.isoformat()
        geotiff_dir = str(run_dir / str(year) / f"data_m{month:02d}")
        add_geotif_command(commands, year, month, geotiff_dir)

        for area_name, area in areas.items():
            for arcsec in arcsecs:
                geotiff_dir_crop = str(run_dir / str(year) / area_name / f"geotiff_crop_{arcsec}")
                output_file = str(run_dir / str(year) / area_name / f"output_r{arcsec}_m{month:02d}.rts")
                add_rts_command(commands, geotiff_dir, geotiff_dir_crop, output_file, arcsec, area)

    start_time = f"{year}-01-01T00:00:00"
    end_time = f"{year}-12-31T23:59:59"
    geotiff_dir = str(run_dir / str(year) / f"data_m")

    for area_name, area in areas.items():
        for arcsec in arcsecs:
            geotiff_dir_crop = str(run_dir / str(year) / area_name / f"geotiff_crop_{arcsec}")
            output_file = str(run_dir / str(year) / area_name / f"output_r{arcsec}.rts")
            add_rts_command(commands, geotiff_dir, geotiff_dir_crop, output_file, arcsec, area)

    # compress the data
    for area_name in areas.keys():
        input_dir = run_dir / str(year) / area_name
        commands.append(f"cd {input_dir.parent} && tar -czf {input_dir.name}.tar.gz {input_dir.name}")

    # upload the file
    commands.append(f"""
        dotenv run python dtran/dcat/scripts/upload_files_in_batch.py upload_files \
            --server=OWNCLOUD --dir={run_dir / str(year)} \
            --ext=tar.gz --upload_dir=Topoflow/GPM_version_2/{year}
    """)
    commands.append(f"rm -rf {run_dir / str(year)}")

    for cmd in tqdm(commands, desc=f"run commands for year {year}"):
        output = subprocess.check_output(cmd, shell=True, cwd=str(HOME_DIR))