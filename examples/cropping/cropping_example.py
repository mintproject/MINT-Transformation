from funcs.trans_cropping_func import *
from drepr import DRepr, outputs

curr_dir = "./"

dataset_file = curr_dir + "3B-MO.MS.MRG.3IMERG.20080101-S000000-E235959.01.V06B.HDF5.nc4"
varname = "atmosphere_water__precipitation_mass_flux"
drepr_file = curr_dir + "gpm.yml"

shape_file = curr_dir + "south-west-shewa-alem-gena.shp"
shape_drepr = curr_dir + "woreda_shapefile.yml"

# bounds
x_min = -78.56
x_max = 82.96
y_min = -164.36
y_max = -65.96


def use_bb(variable_name, dataset):
    func = CroppingTransFunc(variable_name, dataset, xmin=x_min, ymin=y_min, xmax=x_max, ymax=y_max)
    return func.exec()


def use_shp_model(variable_name, dataset, shape_dataset):
    func = CroppingTransFunc(variable_name, dataset, shape=shape_dataset)
    return func.exec()


if __name__ == "__main__":
    dataset = outputs.ArrayBackend.from_drepr(drepr_file, dataset_file)

    shape_model = DRepr.parse_from_file(shape_drepr)
    shape_dataset = outputs.ArrayBackend.from_drepr(shape_model, shape_file)

    # result = use_bb(varname, dataset)

    result = use_shp_model(varname, dataset, shape_dataset)

    print(result)
