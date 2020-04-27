# MINT Data Transformations

A framework to construct a transformation pipeline based on some specification from users.

## Installation

```
git clone https://github.com/mintproject/MINT-Transformation.git
cd MINT-Transformation
conda env create -f environment.yml
```



## Usage

Activate conda enviroment:
```
conda activate mintdt
```

Run the pipeline:

```
dotenv run python -m dtran.main exec_pipeline --config [config_path]
```

You can replace `config_path` with any configuration file found in the [examples](https://github.com/mintproject/MINT-Transformation/tree/master/examples) folder. Some example transformations are:
- [Topoflow](https://github.com/mintproject/MINT-Transformation/blob/master/examples/topoflow4/topoflow_climate.yml)
- [Pihm2Cycles](https://github.com/mintproject/MINT-Transformation/blob/master/examples/pihm2cycles/config.yml)
- [Weather data cropping](https://github.com/mintproject/MINT-Transformation/blob/master/examples/cropping_weather_dataset.yml)


### Developers:

See the first revision of a demo notebook in file [`demo.ipynb`](https://github.com/mintproject/MINT-Transformation/blob/master/examples/demo.ipynb).

### General Users:

#### Start Server

Run the following command from the root folder:

```
PYTHONPATH=$(pwd)/webapp:$(pwd) dotenv run python webapp/api/app.py
```

Open URL `http://0.0.0.0:10010` on your browser

## Running using Docker

Build image

```
docker build -t mint_dt .
```

Run image with local mount and port 5000 exposed

```
docker run --rm -p 5000:5000 -v $(pwd):/ws -it mint_dt bash
```
## Service

We have a deployed transformation service running [here](https://data-trans.mint.isi.edu/). Demo video on how to use the service can be found [here](https://drive.google.com/file/d/1YCPCV2dVbkju_haY8Gj9YxTUpADyMKhT/view)
