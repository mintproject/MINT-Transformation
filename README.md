# MINT Data Transformations

A framework to construct a transformation pipeline based on some specification from users.

## Installation
```
#TODO
```

## Usage

### Developers:
See the first revision of a demo notebook in file `demo.ipynb`.

### General Users:
#### Start Server
Run the following command from the root folder:
```
PYTHONPATH=$(pwd):$(pwd)/extra_libs:$PYTHONPATH python flaskr/app.py
```
Run the following command from the /webapp:
```
npm start
```
Open URL `http://0.0.0.0:3000` on your browser

## Running using Docker
Build image
```
docker build -t mint_dt .
```
Run image with local mount and port 5000 exposed
```
docker run --rm -p 5000:5000 -v $(pwd):/ws -it mint_dt bash
```
Run the UI
```
cd ws && python ui/app.py
```