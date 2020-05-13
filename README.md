# MINT Data Transformations

A framework to construct a transformation pipeline based on some specification from users.

## Installation

```
git clone https://github.com/mintproject/MINT-Transformation.git
cd MINT-Transformation
conda env create -f environment.yml
```

## Usage
### With Conda Environment
Activate conda enviroment:
```
conda activate mintdt
```

Run the pipeline:

```
dotenv -f [env_path] run python -m dtran.main exec_pipeline --config [config_path]
```
Arguments:
  * `env_path`: Path of .env file ([sample](https://github.com/mintproject/MINT-Transformation/blob/master/.env.docker)).
  * `config_path`: Path to the transformation pipeline configuration file ([Topoflow example](https://github.com/mintproject/MINT-Transformation/blob/master/examples/topoflow4/topoflow_climate.yml)).

### With Docker container:
Run the pipeline:

```
docker run --rm -v $(pwd):/ws -v /tmp:/tmp mint_dt [config_path]
```
External files should be stored in `/tmp`
## Deployment

See the first revision of a demo notebook in file [`demo.ipynb`](https://github.com/mintproject/MINT-Transformation/blob/master/examples/demo.ipynb).


### Start Server

Run the following command from the root folder:

```
PYTHONPATH=$(pwd)/webapp:$(pwd) dotenv run python webapp/api/app.py
```

Open URL `http://0.0.0.0:10010` on your browser

### Running using Docker

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
