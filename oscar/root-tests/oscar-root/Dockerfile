FROM rootproject/root:6.26.02-ubuntu22.04

RUN apt update && apt install -y jq python3-pip && apt clean && rm -rf /var/lib/apt/lists/*
RUN pip3 install cloudpickle

COPY root-map.sh /opt/root-map.sh
COPY python-runner.py /opt/python-runner.py

