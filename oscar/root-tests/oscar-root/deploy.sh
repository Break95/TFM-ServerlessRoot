#!/bin/bash

docker build -t xbr34k/root-oscar-2 .
docker push xbr34k/root-oscar-2
oscar-cli apply root-oscar-service.yaml
