# MINT Data Transformations

A framework to construct a transformation pipeline based on some specification from users.

## Installation

The easiest way to install and use the software is using docker:

1. Clone the repository

```
git clone https://github.com/mintproject/MINT-Transformation.git
cd MINT-Transformation
```

2. Build docker image

```
docker build -t mint_dt .
```

However, you can directly install the software without docker by replacing the second step with:

```
conda env create -f environment.yml
```

**Post installation steps:** (will be removed in the future)

```
mkdir /tmp
chmod 1777 /tmp
```

## Usage

You can use the software through the command line application or through the web application.

### Command line application

**With conda environment**

1. Activate the environment first

```
conda activate mintdt
```

2. Run the pipeline:

```
dotenv -f [env_path] run python -m dtran.main exec_pipeline --config [config_path]
```
Arguments:
  * `env_path`: Path of .env file ([sample](https://github.com/mintproject/MINT-Transformation/blob/master/.env.docker)).
  * `config_path`: Path to the transformation pipeline configuration file ([Topoflow example](https://github.com/mintproject/MINT-Transformation/blob/master/examples/topoflow4/topoflow_climate.yml)).

**With docker**

```
docker run --rm -v $(pwd):/ws -v /tmp:/tmp mint_dt [config_path]
```

### Web application

**With conda environment**
1. Start the server by running the following command from the root folder:

```
PYTHONPATH=$(pwd)/webapp:$(pwd) dotenv run python webapp/api/app.py
```

Open URL `http://0.0.0.0:10010` on your browser

**With docker**

Run image with local mount and port 5000 exposed

```
docker run --rm -p 5000:5000 -v $(pwd):/ws -it mint_dt bash
```

**Public server**

We have a deployed transformation service running [here](https://data-trans.mint.isi.edu/). Demo video on how to use the service can be found [here](https://drive.google.com/file/d/1YCPCV2dVbkju_haY8Gj9YxTUpADyMKhT/view)
