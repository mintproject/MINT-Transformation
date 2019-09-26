FROM continuumio/miniconda3

# add linux build essentials
RUN apt-get update
RUN apt-get install -y linux-headers-amd64 build-essential

# update conda
RUN conda update -n base -c defaults conda

# run conda Topoflow4 environment
ADD environment.yml /tmp/environment.yml
RUN conda env create -f /tmp/environment.yml