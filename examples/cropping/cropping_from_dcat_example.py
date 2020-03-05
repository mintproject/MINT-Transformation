from funcs.readers.dcat_read_func import *
from funcs.trans_cropping_func import CroppingTransFunc
from datetime import datetime

from funcs.writers.write_func import CSVWriteFunc

varname = "atmosphere_water__precipitation_mass_flux"

precipitation_dcat_id = "ea0e86f3-9470-4e7e-a581-df85b4a7075d"
shape_dcat_id = "74e6f707-d5e9-4cbd-ae26-16ffa21a1d84"

start = datetime(2018, 1, 1)
end = datetime(2018, 9, 30)

if __name__ == "__main__":
    precipitation_data_reader = DcatReadFunc(precipitation_dcat_id, start_time=start, end_time=end)
    shape_data_reader = DcatReadFunc(shape_dcat_id)

    # TODO Seems there's no way to provide preference to a reader currently (Should this be in metadata?)
    precipitation_data_reader.set_preferences({"data": "array"})
    shape_data_reader.set_preferences({"data": "array"})

    precipitation_dataset = precipitation_data_reader.exec()
    shape_dataset = shape_data_reader.exec()

    crop_func = CroppingTransFunc(varname, precipitation_dataset['data'], shape_dataset['data'])
    cropped_data = crop_func.exec()

    write_func = CSVWriteFunc(cropped_data["data"], "../demo/cropped_result.csv")
    write_func.exec()
