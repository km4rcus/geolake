FROM rg.nl-ams.scw.cloud/dds-production/geokube:v0.2a2
WORKDIR /code
COPY ./api/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY ./utils/wait-for-it.sh /code/wait-for-it.sh
COPY ./datastore /code/app/datastore
COPY ./db/dbmanager /code/db/dbmanager
COPY ./geoquery/ /code/geoquery
COPY ./resources /code/app/resources
COPY ./api/app /code/app
EXPOSE 80
CMD ["uvicorn", "app.main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "80"]