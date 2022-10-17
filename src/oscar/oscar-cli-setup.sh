#!/usr/bin/env bash
# Setup oscar cli to local cluster. Pass user and password as arguments.
# Default setup user and password are: oscar - oscarPassword
if [ -z "$1" ]; then
   oscar-cli cluster add root-cluster http://localhost oscar oscarPassword --disable-ssl
else
    oscar-cli cluster add root-cluster http://localhost $1 $2 --disable-ssl
fi

# This should return info about the cluster.
oscar-cli cluster info
