import glob

from drepr import DRepr, outputs
from tqdm import tqdm

from dtran.backend import ShardedBackend
from funcs import CSVWriteFunc

HOME_DIR = "/"
variable = "land_surface_air__temperature"
date_pattern = "201109"


def read_local_datasets(repr_file, resource_path):
    drepr = DRepr.parse_from_file(repr_file)
    files = glob.glob(resource_path)

    if len(files) == 1:
        return outputs.ArrayBackend.from_drepr(drepr, files[0])

    ds = ShardedBackend(len(files))
    for file in tqdm(files):
        ds.add(outputs.ArrayBackend.from_drepr(drepr, file, ds.inject_class_id))
    return ds


temp_dataset = read_local_datasets(
    HOME_DIR + "/demo/data/gldas/gldas.crop.yml",
    HOME_DIR + f"/demo/data/gldas/{variable}/{date_pattern}*.nc4",
)

write_func = CSVWriteFunc(temp_dataset, "cropped_result.csv")

write_func.exec()
