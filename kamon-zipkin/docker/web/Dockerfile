FROM elyrixia/zipkin-base:master
MAINTAINER Elyrixia "https://github.com/Elyrixia"

ADD Main.scala /zipkin/zipkin-web/src/main/scala/com/twitter/zipkin/web/Main.scala
ADD trace.mustache /zipkin/zipkin-web/src/main/resources/templates/v2/trace.mustache
ADD run.sh /usr/local/bin/run
ENTRYPOINT ["/usr/local/bin/run"]

EXPOSE 8585
