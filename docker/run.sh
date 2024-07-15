#!/bin/bash

source ./docker/names.config

if [[ $# -eq 1 ]]; then

    if [[ $1 -eq "-i" ]]; then
        docker run --rm -it -p 55555:55555 -p 55556:55556 --name=$SERVER_CONTAINER_NAME $SERVER_IMAGE_NAME:latest
    fi

else
    docker run --rm -d  -p 55555:55555 -p 55556:55556 --name=$SERVER_CONTAINER_NAME $SERVER_IMAGE_NAME:latest
fi

