#!/usr/bin/env sh

# Base root image and backend
cd ./root-backend && ./build-and-push.sh

# Client
cd ../root-client && ./build-and-push.sh

# Services
cd ../root-services
for service in r*/ ; do
    echo "./${service}"
    cd "./${service}"
    ./build-and-push.sh
    cd ..
done
