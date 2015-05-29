FROM debian:sid
MAINTAINER Elyrixia "https://github.com/Elyrixia"

RUN         apt-get update && apt-get install -y redis-server
ENTRYPOINT  ["/usr/bin/redis-server"]

EXPOSE      6380