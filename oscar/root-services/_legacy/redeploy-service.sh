#!/bin/bash

oscar-cli service remove $1
. ./deploy.sh $1

