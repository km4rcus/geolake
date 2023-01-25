FROM rg.nl-ams.scw.cloud/dds-production/geokube:v0.2a5
WORKDIR /code
COPY ./executor/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY ./db/dbmanager /code/app/db/dbmanager
COPY ./utils/wait-for-it.sh /code/wait-for-it.sh
COPY ./datastore /code/app/datastore
COPY ./geoquery /code/app/geoquery
COPY ./resources /code/app/resources
COPY ./executor/app /code/app
CMD [ "python", "./app/main.py" ]