# MINT Data Transformations

A framework to construct a transformation pipeline based on some specification from users.

## Installation

```
#TODO
```

## Usage

Run the pipeline:

```
dotenv run python -m dtran.main exec_pipeline --config ./examples/cropping_weather_dataset.yml
```

### Developers:

See the first revision of a demo notebook in file `demo.ipynb`.

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
