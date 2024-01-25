ARG REGISTRY=rg.nl-ams.scw.cloud/geokube-production
ARG TAG=latest
FROM $REGISTRY/geokube:$TAG
RUN conda install -c conda-forge --yes --freeze-installed intake=0.6.6
RUN conda clean -afy
COPY dist/intake_geokube-0.1a0-py3-none-any.whl /
RUN pip install /intake_geokube-0.1a0-py3-none-any.whl
RUN rm /intake_geokube-0.1a0-py3-none-any.whl
