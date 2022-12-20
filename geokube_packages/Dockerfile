FROM condaforge/miniforge3:22.9.0-2
RUN conda install -c conda-forge --yes --freeze-installed \
    xesmf cartopy psycopg2 \
    'bokeh>=2.4.2,<3' pandas=1.4.3 netCDF4 scipy xarray=2022.6.0 \
    && conda clean -afy
COPY geokube-0.2a0-py3-none-any.whl /
COPY intake_geokube-0.1a0-py3-none-any.whl /
RUN pip install /geokube-0.2a0-py3-none-any.whl
RUN pip install /intake_geokube-0.1a0-py3-none-any.whl
