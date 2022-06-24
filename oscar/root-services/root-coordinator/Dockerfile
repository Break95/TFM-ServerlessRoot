# The coordinator doesn't need the root base image, we can work with much less.
FROM ubuntu:22.04

RUN apt-get update && apt-get -y -q install python3 python3-pip

RUN pip install minio cloudpickle

COPY python-runner.py /opt/python-runner.py
