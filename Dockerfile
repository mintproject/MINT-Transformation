FROM continuumio/miniconda3:4.7.12

# add linux build essentials
RUN apt-get update && apt-get install -y build-essential tree

# run conda Topoflow4 environment
ADD environment.yml /tmp/environment.yml
RUN conda env create -f /tmp/environment.yml

RUN sed -i '$ d' ~/.bashrc && \
    echo "conda activate mintdt" >> ~/.bashrc

RUN bash -c 'source ~/.bashrc && conda activate mintdt'
ADD .docker/entrypoint.sh /
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
