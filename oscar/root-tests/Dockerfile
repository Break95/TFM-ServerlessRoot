FROM rootproject/root

RUN apt update && apt install -y jq && apt clean && rm -rf /var/lib/apt/lists/*

COPY root-map.sh /opt/root-map.sh
COPY python-runner.py /opt/python-runner.py

