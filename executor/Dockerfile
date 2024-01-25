ARG REGISTRY=rg.nl-ams.scw.cloud/geodds-production
ARG TAG=latest
FROM $REGISTRY/geodds-datastore:$TAG
WORKDIR /app
COPY requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir -r /code/requirements.txt
COPY app /app
CMD [ "python", "main.py" ]
