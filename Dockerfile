FROM continuumio/miniconda3

# update conda
RUN conda update -n base -c defaults conda

# TODO: fix the following, it needs to run from bash context

# create Topoflow env
#RUN conda create --name tf4
#RUN conda activate tf4

# install GDAL/OGR and other required libs
#RUN conda install -c conda-forge gdal scipy pydap
#RUN conda install numpy dask

# regrid command
#RUN /opt/conda/envs/tf4/bin/python regrid.py