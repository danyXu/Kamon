#!/bin/bash
if [[ -z $QUERY_PORT_9411_TCP_ADDR ]]; then
  echo "** ERROR: You need to link the query service as query."
  exit 1
fi

SERVICE_NAME="zipkin-web"

echo "** Starting ${SERVICE_NAME}..."
cd zipkin
bin/web
