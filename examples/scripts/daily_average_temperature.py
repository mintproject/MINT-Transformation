import matplotlib.pyplot as plt, rdflib, pandas as pd, numpy as np, sys, os, random, math, fiona, uuid, copy, glob
from osgeo import gdal, osr, gdal_array
from collections import defaultdict, Counter
from dotenv import load_dotenv
from tqdm.auto import tqdm
from typing import *
from ruamel.yaml import YAML
import xarray as xr

load_dotenv(verbose=True)
paths = ["../..", "/workspace/d-repr/pydrepr", "/home/rook/workspace/d-repr/pydrepr"]
for path in paths:
  if path not in sys.path:
    sys.path.insert(0, path)

yaml = YAML()

from drepr import __version__, DRepr, outputs
from drepr.executors.readers.reader_container import ReaderContainer
from drepr.executors.readers.np_dict import NPDictReader
print("drepr version:", __version__)

from funcs import DcatReadFunc
from funcs.trans_cropping_func import CroppingTransFunc
from funcs.readers.dcat_read_func import ShardedClassID, ShardedBackend
from funcs.gdal.raster import *
from dateutil.parser import parse

HOME_DIR = os.environ['HOME_DIR']
print(HOME_DIR)

gldas = "5babae3f-c468-4e01-862e-8b201468e3b5"
gpm = "ea0e86f3-9470-4e7e-a581-df85b4a7075d"
region = "74e6f707-d5e9-4cbd-ae26-16ffa21a1d84"
variable = "atmosphere_water__precipitation_mass_flux"
variable = "land_surface_air__temperature"

ethiopia = BoundingBox(32.75418, 3.22206, 47.98942, 15.15943)

def read_datasets(dataset_id, start_time, end_time):
  if start_time is not None:
    start_time = parse(start_time)
  if end_time is not None:
    end_time = parse(end_time)
    
  func = DcatReadFunc(dataset_id, start_time, end_time)
  func.set_preferences({"data": "array"})
  datasets = func.exec()['data']
  return datasets

def read_local_datasets(repr_file, resource_path):
  drepr = DRepr.parse_from_file(repr_file)
  files = glob.glob(resource_path)
  
  if len(files) == 1:
    return outputs.ArrayBackend.from_drepr(drepr, file)[0]
  
  ds = ShardedBackend(len(files))
  for file in tqdm(files):
    ds.add(outputs.ArrayBackend.from_drepr(drepr, file, ds.inject_class_id))
  return ds

with open(HOME_DIR + "/examples/d3m/crop_bb.yml", "r") as f:
  crop_bb_conf = yaml.load(f)

def dataset2raster(sm, variable):
  rasters = []
  for c in sm.c('mint:Variable').filter(outputs.FCondition("mint:standardName", "==", variable)):
    for raster_id, sc in c.group_by("mint-geo:raster"):
      # TODO: handle time properly
      timestamp = sc.p("mint:timestamp").as_ndarray([])
      if timestamp.data.size != 1:
        raise NotImplemented()
      timestamp = timestamp.data[0]
      
      data = sc.p("rdf:value").as_ndarray([sc.p("mint-geo:lat"), sc.p("mint-geo:long")])
      gt_info = sm.get_record_by_id(raster_id)
      gt = GeoTransform(x_0=gt_info.s("mint-geo:x_0"),
                        y_0=gt_info.s("mint-geo:y_0"),
                        dx=gt_info.s("mint-geo:dx"), dy=gt_info.s("mint-geo:dy"))
      raster = Raster(data.data, gt, int(gt_info.s("mint-geo:epsg")),
             float(data.nodata.value) if data.nodata is not None else None)
      raster.timestamp = timestamp
      rasters.append(raster)
  return rasters
  
def raster2dataset(r, variable):
  global crop_bb_conf
  reader = NPDictReader({
    "variable": r.data,
    "lat": r.get_center_latitude(),
    "long": r.get_center_longitude(),
    "timestamp": r.timestamp,
    "standard_name": variable,
    "gt_x_0": r.geotransform.x_0,
    "gt_y_0": r.geotransform.y_0,
    "gt_dx": r.geotransform.dx,
    "gt_dy": r.geotransform.dy,
    "gt_epsg": r.epsg,
    "gt_x_slope": r.geotransform.x_slope,
    "gt_y_slope": r.geotransform.y_slope,
  })
  resource_id = str(uuid.uuid4())
  ReaderContainer.get_instance().set(resource_id, reader)
  
  conf = copy.deepcopy(crop_bb_conf)
  conf['attributes']['variable']['missing_values'].append(r.nodata)
  drepr = DRepr.parse(conf)
  sm = outputs.ArrayBackend.from_drepr(drepr, resource_id)
  ReaderContainer.get_instance().delete(resource_id)
  return sm

def raster2netcdf(r, variable, outfile):
  lat = r.get_center_latitude()
  long = r.get_center_longitude()
  data = xr.DataArray(r.data, dims=('lat', 'long'), coords={'lat': lat, 'long': long})
  data.attrs['standard_name'] = variable
  data.attrs['_FillValue'] = r.nodata
  data.attrs['missing_values'] = r.nodata
  
  ds = xr.Dataset({standard_name: data})  
  ds.to_netcdf(outfile)
  
def dataset2netcdf(sm):
  assert len(sm.c("mint:Variable")) == 1
  c = sm.c("mint:Variable")[0]
  if c.p("mint:Place") is not None:
    raise NotImplemented()
    
  standard_name = c.p("mint:standardName").as_ndarray([]).data
  assert standard_name.size == 1
  standard_name = standard_name[0]

  timestamp = c.p("mint:timestamp").as_ndarray([]).data
  assert timestamp.size == 1
  timestamp = timestamp[0]
  
  groups = list(c.group_by("mint-geo:raster"))
  assert len(groups) == 1
  gt = sm.get_record_by_id(groups[0][0])

  val = c.p("rdf:value").as_ndarray([c.p("mint-geo:lat"), c.p("mint-geo:long")])
  data = val.data.reshape(1, *val.data.shape)
  data = xr.DataArray(val.data.reshape(1, *val.data.shape), dims=('time', 'lat', 'long'), coords={
    'lat': val.index_props[0], 'long': val.index_props[1], 'time': np.asarray([timestamp])
  })
  data.attrs['standard_name'] = standard_name
  data.attrs['_FillValue'] = val.nodata.value
  data.attrs['missing_values'] = val.nodata.value
  
  ds = xr.Dataset({"variable": data})
  ds.attrs.update({
    "conventions": "CF-1.6",
    "dx": gt.s('mint-geo:dx'),
    "dy": gt.s("mint-geo:dy"),
    "epsg": gt.s("mint-geo:epsg"),
    "x_slope": gt.s("mint-geo:x_slope"),
    "y_slope": gt.s("mint-geo:y_slope"),
    "x_0": gt.s("mint-geo:x_0"),
    "y_0": gt.s("mint-geo:y_0")
  })
  return ds

def shape_array_to_shapefile(data, fname):
  polygon = data[0]
  if isinstance(polygon[0][0][0], (int, float)):
    shape_type = 'Polygon'
  else:
    shape_type = 'MultiPolygon'

  epsg = fiona.crs.from_epsg(data[1])
  driver = "ESRI Shapefile"
  polygon = {
      'geometry': {
          'type': shape_type,
          'coordinates': polygon
      },
      'properties': {
          'name': 'TempCroppingPolygon'
      }
  }
  schema = {'geometry': shape_type, 'properties': {'name': 'str'}}
  with fiona.open(fname, 'w', crs='+datum=WGS84 +ellps=WGS84 +no_defs +proj=longlat', driver=driver, schema=schema) as shapefile:
    shapefile.write(polygon)
    
def create_shapefile(sm, dname, randomize: bool=False):
  shape_files = []
  random_id = str(uuid.uuid4())
  for c in sm.c("mint:Place"):
    for r in c.iter_records():
      polygon = sm.get_record_by_id(r.s('mint-geo:bounding')).s('rdf:value')
      if randomize:
        shape_file = HOME_DIR + f'/data/{dname}/{random_id}/{r.s("mint:region").replace(" ", "-")}.shp'
      else:
        shape_file = HOME_DIR + f'/data/{dname}/{r.s("mint:region").replace(" ", "-")}.shp'
#       print(shape_file)
      Path(shape_file).parent.mkdir(exist_ok=True, parents=True)
      shape_array_to_shapefile([polygon, 4326], shape_file)
      shape_files.append({
        "file": shape_file,
        "region": r.s("mint:region")
      })
  return shape_files

date_pattern = sys.argv[1]
temp_dataset = read_local_datasets(HOME_DIR + "/examples/d3m/gldas.crop.yml", 
                                   HOME_DIR + f"/data/gldas/{variable}/{date_pattern}*.nc4")

region_dataset = read_datasets(region, None, None)
shape_files = create_shapefile(region_dataset, 'debug/regions-random', True)

temp_data = []

for raster in tqdm(dataset2raster(temp_dataset, variable), desc='cropping'):
  for shape_file in shape_files:
    sr = raster.crop(vector_file=shape_file['file'],
                      resampling_algo=ReSample.BILINEAR,
                      touch_cutline=True)
    temp_data.append({
      "data": sr.data,
      "nodata": sr.nodata,
      "region": shape_file['region'],
      "timestamp": raster.timestamp,
      "timestring": datetime.datetime.utcfromtimestamp(raster.timestamp).strftime("%Y%m%d%H%M%S"),
      "date": datetime.datetime.utcfromtimestamp(raster.timestamp).strftime("%Y-%m-%d")
    })

groups = defaultdict(list)
for d in tqdm(temp_data):
  groups[(d['date'], d['region'])].append(d)

rows = []

i = 0
for (date, region), meshs in groups.items():
  i += 1
  n_obs_mesh = np.zeros(meshs[0]['data'].shape, dtype=np.int64)
  sum_mesh = np.zeros(meshs[0]['data'].shape, dtype=np.float64)
  for mesh in meshs:
    d = mesh['data']
    nodata = mesh['nodata']
    
    mask = (d != nodata).astype(np.int64)
    # count the reported points and their values
    n_obs_mesh += mask
    sum_mesh += mask * d
  
  obs_mask = n_obs_mesh != 0
  avg_daily_temp_mesh = sum_mesh[obs_mask] / n_obs_mesh[obs_mask]
  avg_daily_temp = np.average(avg_daily_temp_mesh) - 273.15
  
  rows.append({"date": date, "region": region, "avg_daily_temp (celsius)": avg_daily_temp})

rows.sort(key=lambda x: (x['date'], x['region']))
df = pd.DataFrame(rows)
df.to_csv(HOME_DIR + f"/data/outputs/avg_daily_temp.{date_pattern}.csv")