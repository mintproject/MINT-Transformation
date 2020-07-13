import subprocess, glob, os, shutil
from tqdm.auto import tqdm
from pathlib import Path

files = [
    "https://files.mint.isi.edu/s/3RZwyxbi5PpcqeV/download",
    "https://files.mint.isi.edu/s/cvY8E1GPC3v8Fi6/download",
    "https://files.mint.isi.edu/s/RzERxVVIh2M7Qzc/download",
    "https://files.mint.isi.edu/s/DswKIJadJHkwiPo/download",
]
download_dir = "/workspace/mint/MINT-Transformation/data/gpm_download"
dest_dir = "/workspace/mint/MINT-Transformation/data/GPM"

cmds = []

# # download the files
# for i, file in enumerate(files):
#     cmds.append(f"wget {file} -O {download_dir}/file_{i}.tar.gz")
# for cmd in tqdm(cmds):
#     subprocess.check_call(cmd, shell=True)

# extract files
# cwd = download_dir
# for file in glob.glob(download_dir + "/*.tar.gz"):
#     cmds.append(f"tar -xzf {file}")
#     print(cmds[-1])
# for cmd in tqdm(cmds):
#     subprocess.check_call(cmd, shell=True)

# copy files
cwd = download_dir
for year in tqdm(range(2008, 2021)):
    (Path(dest_dir) / str(year)).mkdir(exist_ok=True, parents=True)
    download_files = glob.glob(download_dir + f"/*/*3IMERG.{year}*")
    print(year, len(download_files))
    for file in download_files:
        shutil.move(file, dest_dir + f'/{year}/')