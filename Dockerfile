FROM continuumio/miniconda3:4.8.2

# add linux build essentials
RUN apt-get update && apt-get install -y build-essential tree

# install environment
ADD environment.yml /tmp/environment.yml
RUN conda env create -f /tmp/environment.yml

RUN apt-get install -y unzip

RUN sed -i '$ d' ~/.bashrc && \
    echo "conda activate mintdt" >> ~/.bashrc

RUN bash -c 'source ~/.bashrc && conda activate mintdt'
ADD .docker/entrypoint.sh /
RUN mkdir /ws
ADD . /ws 
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
