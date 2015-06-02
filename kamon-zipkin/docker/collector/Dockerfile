FROM elyrixia/zipkin-base:master
MAINTAINER Elyrixia "https://github.com/Elyrixia"

ADD collector-redis.scala /zipkin/zipkin-collector-service/config/collector-redis.scala
ADD run.sh /usr/local/bin/run
ENTRYPOINT ["/usr/local/bin/run"]

EXPOSE 9410