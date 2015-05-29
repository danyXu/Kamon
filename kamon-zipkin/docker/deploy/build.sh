#!/bin/bash
PREFIX="elyrixia/zipkin-"
IMAGES=("base" "collector" "query" "web")

for image in ${IMAGES[@]}; do
  pushd "../$image"
  docker build --no-cache -t "$PREFIX$image:master" .
  popd
done
