#!/bin/bash

docker build -t xbr34k/root-faas-demo .
docker push xbr34k/root-faas-demo
oscar-cli apply root-service.yaml
