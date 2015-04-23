#!/bin/bash
IMG_PREFIX="elyrixia/zipkin-"
NAME_PREFIX="zipkin-"
PUBLIC_PORT="8585"
ROOT_URL="http://deb.local:$PUBLIC_PORT"
CLEANUP="y"

if [[ $CLEANUP == "y" ]]; then
  SERVICES=("collector" "query" "web")
  for i in "${SERVICES[@]}"; do
    echo "** Stopping zipkin-$i"
    docker stop "${NAME_PREFIX}$i"
    docker rm "${NAME_PREFIX}$i"
  done
fi

echo "** Starting zipkin-collector"
docker run -d --link="redis:db" -p 9410:9410 --name="${NAME_PREFIX}collector" "${IMG_PREFIX}collector:master"

echo "** Starting zipkin-query"
docker run -d --link="redis:db" -p 9411:9411 --name="${NAME_PREFIX}query" "${IMG_PREFIX}query:master"

echo "** Starting zipkin-web"
docker run -d --link="${NAME_PREFIX}query:query" -p 8585:$PUBLIC_PORT -e "ROOTURL=${ROOT_URL}" --name="${NAME_PREFIX}web" "${IMG_PREFIX}web:master"
