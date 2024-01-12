# vim:set ft=dockerfile:
FROM continuumio/miniconda3
MAINTAINER https://github.com/roocs/rook
LABEL Description="rook WPS" Vendor="Birdhouse" Version="0.8.2"

# Update Debian system
RUN apt-get update && apt-get install -y \
 build-essential \
&& rm -rf /var/lib/apt/lists/*

# Update conda
RUN conda update -n base conda

# Copy WPS project
COPY requirements.txt /opt/wps/requirements.txt
COPY requirements_dev.txt /opt/wps/requirements_dev.txt
COPY environment.yml /opt/wps/environment.yml

WORKDIR /opt/wps

# Create conda environment with PyWPS
RUN ["conda", "env", "create", "-n", "wps", "-f", "environment.yml"]

# Install ddsapi-client 2.0
COPY geokube_packages/ddsapi-0.5b1-py3-none-any.whl /packages/ddsapi-0.5b1-py3-none-any.whl
RUN ["/bin/bash", "-c", "source activate wps && pip install /packages/ddsapi-0.5b1-py3-none-any.whl --no-cache"]

# Install WPS
COPY rook ./rook
COPY setup.py ./setup.py
COPY README.rst ./README.rst
COPY CHANGES.rst ./CHANGES.rst
RUN ["/bin/bash", "-c", "source activate wps && pip install -e ."]
COPY project_utils.py ../conda/envs/wps/lib/python3.9/site-packages/roocs_utils/project_utils.py

# Export ROOCS_CONFIG
COPY .custom.cfg ./.custom.cfg
COPY rook/default.cfg ./rook/default.cfg
COPY etc ./etc
ENV ROOCS_CONFIG=/opt/wps/etc/roocs.ini
RUN export ROOCS_CONFIG=/opt/wps/etc/roocs.ini && echo "$ROOCS_CONFIG"

RUN mkdir -p /opt/wps/outputs

# Start WPS service on port 5000 on 0.0.0.0
EXPOSE 5000
# EXPOSE 8182
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["source activate wps && exec rook start --bind-host 0.0.0.0 -c /opt/wps/etc/custom.cfg"]
# rook start -c etc/custom.cfg --bind-host 0.0.0.0 --daemon

# docker build -t roocs/rook .
# docker run --name rook -p 8182:8182 -v /data:/data -v /work/services/dds:/work/services/dds -v /home/vale/outputs:/opt/wps/outputs roocs/rook &
# docker run -p 5000:5000 roocs/rook
# http://localhost:5000/wps?request=GetCapabilities&service=WPS
# http://localhost:5000/wps?request=DescribeProcess&service=WPS&identifier=all&version=1.0.0
