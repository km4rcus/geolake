FROM rg.nl-ams.scw.cloud/dds-production/geokube:v0.2a0
WORKDIR /code
COPY ./api/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY ./utils/wait-for-it.sh /code/wait-for-it.sh
COPY ./datastore /code/app/datastore
COPY ./db/dbmanager /code/db/dbmanager
COPY ./geoquery/ /code/geoquery
COPY ./resources /code/app/resources
COPY ./api/app /code/app
# COPY ./api/tests /code/tests
# RUN pytest /code/tests
EXPOSE 80
# VOLUME /code
# CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
# if behind a proxy use --proxy-headers
CMD ["uvicorn", "app.main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "80"]