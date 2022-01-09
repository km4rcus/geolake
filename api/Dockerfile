FROM continuumio/miniconda3
WORKDIR /code
COPY ./api/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
RUN conda install -c anaconda psycopg2
COPY ./utils/wait-for-it.sh /code/wait-for-it.sh
COPY ./db/dbmanager /code/db/dbmanager
COPY ./geoquery/ /code/geoquery
COPY ./api/app /code/app
EXPOSE 80
# VOLUME /code
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
# if behind a proxy use --proxy-headers
# CMD ["uvicorn", "app.main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "80"]