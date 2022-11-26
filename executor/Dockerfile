FROM continuumio/miniconda3
WORKDIR /code
RUN conda install -c conda-forge --yes --freeze-installed \
    xesmf cartopy psycopg2 \
    netCDF4 scipy xarray=2022.3.0 \
    && conda clean -afy
COPY ./executor/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY geokube_packages/geokube-0.2a0-py3-none-any.whl /code
COPY geokube_packages/intake_geokube-0.1a0-py3-none-any.whl /code
RUN pip install /code/geokube-0.2a0-py3-none-any.whl
RUN pip install /code/intake_geokube-0.1a0-py3-none-any.whl
COPY ./db/dbmanager /code/app/db/dbmanager
COPY ./utils/wait-for-it.sh /code/wait-for-it.sh
COPY ./datastore /code/app/datastore
COPY ./geoquery /code/app/geoquery
COPY ./resources /code/app/resources
COPY ./executor/app /code/app
CMD [ "python", "./app/main.py" ]