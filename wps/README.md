# geokube-wps
OGC WPS Interface

## geokube Installation 

#### Requirements
You need to install xesmf and cartopy to use geokube. This can be done during the creation of conda virtual environment, as shown below

Add or append conda-forge channel
```bash
conda config --add channels conda-forge
```
or
```bash
conda config --append channels conda-forge
```

#### Conda Environment
Create virtual environment with installing xesmf and cartopy package
```bash
conda create -n geokube-wps python=3.9 xesmf cartopy -y
```
Activate virtual environment
```bash
conda activate geokube-wps
```
Install geokube framework
```bash
pip install ./geokube_packages/geokube-0.1a0-py3-none-any.whl
```

## Rook Installation
```bash
python setup.py install
```