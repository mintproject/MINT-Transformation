# This code compute latitude and longitude of points in PIHM

library('rgdal')
library('rgeos')
library('sp')

args = commandArgs(trailingOnly = TRUE)

x.pcs=readOGR(args[1]);   # Read the Spatial Data
x.gcs = spTransform(x.pcs, CRSargs(CRS("+init=epsg:4326")))     # Reproject the PCS to GCS
lonlat = coordinates(x.gcs)     #get the lon/lat of centroids of the triangles.
write.csv(lonlat, args[2])
