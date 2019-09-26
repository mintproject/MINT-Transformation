FROM ubuntu:16.04
MAINTAINER Basel Shbita "shbita@usc.edu"

# install python3.6 and wget
RUN apt-get update && \
    apt-get install -y software-properties-common wget && \
    add-apt-repository -y ppa:jonathonf/python-3.6
RUN apt-get update -y
RUN apt-get install -y build-essential python3.6 python3.6-dev python3-pip python3.6-venv

# update pip
RUN python3.6 -m pip install pip --upgrade

# install GDAL/OGR
RUN add-apt-repository -y ppa:ubuntugis/ppa && \
    apt-get update -y && \
    apt-get install -y gdal-bin libgdal-dev python-gdal python3-gdal

# download example netcdf file
RUN mkdir examples
ADD https://workflow.isi.edu/MINT/FLDAS/FLDAS_NOAH01_A_EA_D.001/2001/01/FLDAS_NOAH01_A_EA_D.A20010102.001.nc /examples