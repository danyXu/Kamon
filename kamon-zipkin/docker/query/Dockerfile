FROM elyrixia/zipkin-base
MAINTAINER Elyrixia "https://github.com/Elyrixia"

ADD query-redis.scala /zipkin/zipkin-query-service/config/query-redis.scala
ADD run.sh /usr/local/bin/run
ENTRYPOINT ["/usr/local/bin/run"]

EXPOSE 9411