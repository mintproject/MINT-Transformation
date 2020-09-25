"""
This script downloads CHIRPS global daily dataset and register with dcat
"""
import os
import urllib

CHIRPS_BASE_URL = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p25"
CHIRPS_DOWNLOAD_DIR = "/Users/summ7t/dev/MINT/MINT-Transformation/data/download/chirps"

if not os.path.exists(CHIRPS_DOWNLOAD_DIR):
    os.makedirs(CHIRPS_DOWNLOAD_DIR)

# Construct netcdf file download url
chirps_urls = [f"{CHIRPS_BASE_URL}/chirps-v2.0.{year}.days_p25.nc" for year in range(1981, 2021, 1)]

for url in chirps_urls:
    chirps_fn = os.path.join(CHIRPS_DOWNLOAD_DIR, url.split("/")[-1])
    print(f"Downloading {url} to {chirps_fn}...")
    urllib.request.urlretrieve(url, chirps_fn)