FROM continuumio/miniconda3
RUN conda install -c conda-forge --yes --freeze-installed \
    cartopy xesmf=0.6.3 psycopg2 \
    'bokeh>=2.4.2,<3' numpy=1.23.5 pandas=1.4.3 netCDF4 scipy xarray=2022.6.0 \
    && conda clean -afy
COPY geokube-0.2a0-py3-none-any.whl /
COPY intake_geokube-0.1a0-py3-none-any.whl /
RUN pip install /geokube-0.2a0-py3-none-any.whl
RUN pip install /intake_geokube-0.1a0-py3-none-any.whl
