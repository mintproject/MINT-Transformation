FROM continuumio/miniconda3

# update conda
RUN conda update -n base -c defaults conda

# run conda Topoflow4 environment
ADD environment.yml /tmp/environment.yml
RUN conda env create -f /tmp/environment.yml

# run TopoFlow4 env
# CMD conda activate tf4