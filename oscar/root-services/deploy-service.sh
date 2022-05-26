#!/bin/bash

cd $1
. ./build-and-push.sh
oscar-cli apply oscar-service.yaml
