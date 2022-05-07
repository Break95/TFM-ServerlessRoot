FROM rootproject/root:6.26.02-ubuntu22.04

RUN apt update && apt install -y jq && apt clean && rm -rf /var/lib/apt/lists/*

COPY root-map.sh /opt/root-map.sh
COPY python-runner.py /opt/python-runner.py

