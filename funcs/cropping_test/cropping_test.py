from funcs.trans_cropping_func import *
from drepr import DRepr, outputs

curr_dir = "./"

dataset_file = curr_dir + "3B-MO.MS.MRG.3IMERG.20080101-S000000-E235959.01.V06B.HDF5.nc4"
varname = "precipitation_flux"
drepr_file = curr_dir + "gpm.yml"

shape_file = curr_dir + "south-west-shewa-alem-gena.shp"
shape_drepr = ""
#shape_dataset = curr_dir + "shape_file.csv" # TODO Wait for Binh for model

# bounds
x_min = -78.56
x_max = 82.96
y_min = -164.36
y_max = -65.96

def use_bb(variable_name, dataset):
    func = CroppingTransFunc(variable_name, dataset, None, x_min, y_min, x_max, y_max)
    return func.exec()

def use_shp_direct(variable_name, dataset, shape_file):
    func = CroppingTransFunc(variable_name, dataset, shape_file, 0, 0, 0 , 0)
    return func.exec()

def use_shp_model(variable_name, dataset, shape_dataset):
    func = CroppingTransFunc(variable_name, dataset, shape_dataset, 0, 0 , 0, 0)
    return func.exec()

if __name__ == "__main__":
    dataset = outputs.ArrayBackend.from_drepr(drepr_file, dataset_file)

    shape_dataset = outputs.ArrayBackend.from_drepr(shape_drepr, shape_file)

    result = use_bb(varname, dataset, x_min, y_min, x_max, y_max)

    #result = use_shp_direct(varname, dataset, shape_file)

    #result = use_shp_model(varname, dataset, shape_dataset)

    print(result)




