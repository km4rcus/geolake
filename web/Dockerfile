FROM continuumio/miniconda3
WORKDIR /code
COPY ./web/requirements.txt /code/requirements.txt
RUN conda install -c conda-forge xesmf cartopy psycopg2 -y
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY geokube_packages/geokube-0.2a0-py3-none-any.whl /code
COPY geokube_packages/intake_geokube-0.1a0-py3-none-any.whl /code
RUN pip install /code/geokube-0.2a0-py3-none-any.whl
RUN pip install /code/intake_geokube-0.1a0-py3-none-any.whl
COPY ./utils/wait-for-it.sh /code/wait-for-it.sh
COPY ./datastore /code/app/datastore
COPY ./db/dbmanager /code/db/dbmanager
COPY ./geoquery/ /code/geoquery
COPY ./resources /code/app/resources
COPY ./web/app /code/app
COPY ./web/templates /code/templates
COPY ./web/tests /code/tests
# RUN pytest /code/tests
EXPOSE 80
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
# if behind a proxy use --proxy-headers
# CMD ["uvicorn", "app.main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "80"]