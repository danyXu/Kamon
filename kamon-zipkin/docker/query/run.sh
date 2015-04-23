#!/bin/bash
if [[ -z $DB_PORT_6379_TCP_ADDR ]]; then
  echo "** ERROR: You need to link the redis container as db."
  exit 1
fi

echo "** Starting ${SERVICE_NAME}..."
cd zipkin
bin/query redis
