#!/usr/bin/env sh

VERSION="0.0.1"
NAME=localedb

docker build \
       -t "${NAME}:${VERSION}" \
       -t "${NAME}:dev" \
       .
