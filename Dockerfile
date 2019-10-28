FROM continuumio/miniconda3:4.7.12

# add linux build essentials
RUN apt-get update && apt-get install -y build-essential

# run conda Topoflow4 environment
ADD environment.yml /tmp/environment.yml
RUN conda env create -f /tmp/environment.yml

RUN sed -i '$ d' ~/.bashrc && \
    echo "conda activate mintdt" >> ~/.bashrc

ADD entrypoint.sh /
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
