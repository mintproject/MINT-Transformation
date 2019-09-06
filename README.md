# MINT Data Transformations

A framework to construct a transformation pipeline based on some specification from users.

## Cloning

This repository includes __submodules__, you can either add `--recurse-submodules` to the `git clone` command or run the following commands after cloning the repository:
```
git submodule init    # initializes your local configuration file
git submodule update  # fetches all the data
```

## Installation

1. Install D-REPR library: `pip install git+https://github.com/binh-vu/temp.git@pyrepr` 

## Usage

### Developers:
See the first revision of a demo notebook in file `demo.ipynb`.

### General Users:
Run the following command from the root folder:
```
python ui/app.py
```
Open URL `http://0.0.0.0:5000` on your browser