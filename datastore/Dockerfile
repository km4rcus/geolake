ARG REGISTRY=rg.nl-ams.scw.cloud/geokube-production
ARG TAG=latest
FROM $REGISTRY/intake-geokube:$TAG
RUN conda install -c conda-forge --yes --freeze-installed psycopg2 \
    && conda clean -afy
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt    
COPY ./datastore /app/datastore
COPY ./workflow /app/workflow
COPY ./dbmanager /app/dbmanager
COPY ./geoquery /app/geoquery
COPY ./utils /app/utils
COPY ./tests /app/tests
RUN cp /opt/conda/lib/python3.10/site-packages/intake_geokube/geoquery.py /app/geoquery/geoquery.py
COPY ./wait-for-it.sh /

