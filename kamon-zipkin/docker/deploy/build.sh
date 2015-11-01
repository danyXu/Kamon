#!/bin/bash
PREFIX="elyrixia/zipkin-"
IMAGES=("base" "collector" "query" "web")

for image in ${IMAGES[@]}; do
  pushd "../$image"
  docker build --no-cache -t "$PREFIX$image:master" .
  popd
done


# for a interuption, could you offet the docker image for this extension (kamon-zipkin) if you hava ?
# because of running the build script has costed a lot of time and got the error result,i tried . 
# waiting for your answer,thanks
