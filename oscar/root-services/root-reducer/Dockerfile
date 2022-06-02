#FROM rootproject/root:6.26.02-ubuntu22.04
FROM ghcr.io/break95/root-base:latest

RUN apt update && apt install -y jq python3-pip && apt clean && rm -rf /var/lib/apt/lists/*
RUN pip3 install cloudpickle minio requests

COPY root-reduce.sh /opt/root-reduce.sh
COPY serverless-reducer.py /opt/serverless-reducer.py

