from funcs.readers.dcat_read_func import *
from funcs.gdal.trans_cropping_func import CroppingTransFunc
from datetime import datetime

from funcs.writers.write_func import CSVWriteFunc

varname = ""
# varname = "atmosphere_water__precipitation_mass_flux"

precipitation_dcat_id = "ea0e86f3-9470-4e7e-a581-df85b4a7075d"
shape_dcat_id = "74e6f707-d5e9-4cbd-ae26-16ffa21a1d84"

start = datetime(2018, 1, 1)
end = datetime(2018, 2, 1)

if __name__ == "__main__":
    precipitation_data_reader = DcatReadFunc(precipitation_dcat_id, start_time=start, end_time=end)
    shape_data_reader = DcatReadFunc(shape_dcat_id)

    precipitation_data_reader.set_preferences({"data": "array"})
    shape_data_reader.set_preferences({"data": "array"})

    precipitation_dataset = precipitation_data_reader.exec()
    shape_dataset = shape_data_reader.exec()

    crop_func = CroppingTransFunc(precipitation_dataset['data'], varname, shape_dataset['data'])
    # crop_func = CroppingTransFunc(precipitation_dataset['data'], varname, None, xmin=30, xmax=50, ymin=0, ymax=20,
    # region_label = "Ethiopia")
    cropped_data = crop_func.exec()

    write_func = CSVWriteFunc(cropped_data["data"], "../demo/data/cropped_result.csv")
    write_func.exec()
