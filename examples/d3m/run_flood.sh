#!/usr/bin/env bash
set -e

export PYTHONPATH=/home/rook/workspace/mint/MINT-Transformation
datadir=/home/rook/workspace/mint/MINT-Transformation/data
#datadir=/data

python run_flood.py $datadir/flooding/flood.2017.nc $datadir/ethiopia-district-l3 $datadir/flooding/output/2017.csv 2017
python run_flood.py $datadir/flooding/flood.2016.nc $datadir/ethiopia-district-l3 $datadir/flooding/output/2016.csv 2016
python run_flood.py $datadir/flooding/flood.2015.nc $datadir/ethiopia-district-l3 $datadir/flooding/output/2015.csv 2015
python run_flood.py $datadir/flooding/flood.2014.nc $datadir/ethiopia-district-l3 $datadir/flooding/output/2014.csv 2014
python run_flood.py $datadir/flooding/flood.2013.nc $datadir/ethiopia-district-l3 $datadir/flooding/output/2013.csv 2013
python run_flood.py $datadir/flooding/flood.2012.nc $datadir/ethiopia-district-l3 $datadir/flooding/output/2012.csv 2012
python run_flood.py $datadir/flooding/flood.2011.nc $datadir/ethiopia-district-l3 $datadir/flooding/output/2011.csv 2011
python run_flood.py $datadir/flooding/flood.2010.nc $datadir/ethiopia-district-l3 $datadir/flooding/output/2010.csv 2010
python run_flood.py $datadir/flooding/flood.2009.nc $datadir/ethiopia-district-l3 $datadir/flooding/output/2009.csv 2009
python run_flood.py $datadir/flooding/flood.2008.nc $datadir/ethiopia-district-l3 $datadir/flooding/output/2008.csv 2008