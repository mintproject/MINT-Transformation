#!/usr/bin/python
# -*- coding: utf-8 -*-
from pathlib import Path
from netCDF4 import Dataset
from datetime import datetime
import numpy as np, os, time
from typing import Union, List
from tqdm import tqdm
import re
import subprocess

from dtran.argtype import ArgType
from dtran.ifunc import IFunc
from os import listdir
from os.path import join

import gdal, osr  ## ogr
import glob
from scipy.special import gamma

from funcs.topoflow.rti_files import generate_rti_file
from funcs.topoflow.write_topoflow4_climate_func import create_rts_from_nc_files
from funcs.readers.dcat_read_func import DCatAPI

DOWNLOAD_PATH = "/data/mint/dcat_gpm"

class DcatReadTopoflow4ClimateUploadFunc(IFunc):
    id = "topoflow4_climate_write_func"
    description = ''' An entry point in the pipeline.
    Fetches a GPM dataset and creates an RTS (and RTI) file from NetCDF (climate) files.
    '''
    inputs = {
        "dataset_id": ArgType.String,
        "DEM_bounds": ArgType.String,
        "DEM_xres_arcsecs": ArgType.String,
        "DEM_yres_arcsecs": ArgType.String,
        "DEM_ncols": ArgType.String,
        "DEM_nrows": ArgType.String,
    }
    outputs = {}

    def __init__(self, dataset_id: str, DEM_bounds: str, DEM_xres_arcsecs: str, DEM_yres_arcsecs: str,
                 DEM_ncols: str, DEM_nrows: str):
        
        self.DEM = {
            "bounds": [float(x.strip()) for x in DEM_bounds.split(",")],
            "xres": float(DEM_xres_arcsecs) / 3600.0,
            "yres": float(DEM_yres_arcsecs) / 3600.0,
            "ncols": int(DEM_ncols),
            "nrows": int(DEM_nrows),
        }

        DCAT_URL = "https://api.mint-data-catalog.org"

        results = DCatAPI.get_instance(DCAT_URL).find_dataset_by_id(dataset_id)
        assert len(results) == 1
        
        resource_ids = {"default": results[0]['resource_data_url']}
        Path(DOWNLOAD_PATH).mkdir(exist_ok=True, parents=True)

        for resource_id, resource_url in resource_ids.items():
            
            resource_suffix = resource_url.split('/')[-1]
            file_full_path = f'{DOWNLOAD_PATH}/{dataset_id}.{resource_suffix}'
            subprocess.check_call(f'wget --user datacatalog --password sVMIryVWEx3Ec2 {resource_url} -O {file_full_path}', shell=True)

            input_dir_full_path = f'{DOWNLOAD_PATH}/{dataset_id}'
            Path(input_dir_full_path).mkdir(exist_ok=True, parents=True)
            subprocess.check_call(f'tar -xvzf {file_full_path} -C {input_dir_full_path}/', shell=True)

            self.input_dir = f'{input_dir_full_path}/{listdir(input_dir_full_path)[0]}'

            self.temp_dir = f'{self.input_dir}__' + '_'.join([i.split(' ')[-1] for i in DEM_bounds.split('.')][:-1])
            Path(self.temp_dir).mkdir(exist_ok=True, parents=True)

            self.output_dir = self.temp_dir + f'_output'
            Path(self.output_dir).mkdir(exist_ok=True, parents=True)

            self.output_file = self.output_dir + f'/climate_all.rts'

            break # TODO: currently we assume a single resource

    def exec(self) -> dict:
        create_rts_from_nc_files(self.input_dir, self.temp_dir, self.output_file, self.DEM)

        # tar output files
        output_tar = self.output_dir + '.tar.gz'
        subprocess.check_call(f'tar -czf {output_tar} {self.output_dir}', shell=True)
        
        # upload tar output file
        upload_output = subprocess.check_output(f'curl -sD - --user upload:HVmyqAPWDNuk5SmkLOK2 --upload-file {output_tar} https://publisher.mint.isi.edu', shell=True)

        self.upload_url = f'https://{upload_output.decode("utf-8").split("https://")[-1]}'
        self.ui_message = f' Transformed data is ready here: {self.upload_url}'

        # TODO: this is a hack to pass UI messages
        with open('/tmp/ui_messages.txt', 'w') as msg_file:
            msg_file.write(self.ui_message)

        return {}

    def validate(self) -> bool:
        return True