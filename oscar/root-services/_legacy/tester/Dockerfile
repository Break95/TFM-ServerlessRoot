FROM bitnami/minideb:bullseye

ENV LANG C.UTF-8

RUN apt update && \
    apt install -y --no-install-recommends cowsay jq libexpat1 && \
    apt install -y python3 pip &&\
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install psutil

COPY psutil_test.py /root/psutil_test.py
