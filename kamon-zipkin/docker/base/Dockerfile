FROM debian:sid
MAINTAINER Elyrixia "https://github.com/Elyrixia"

RUN apt-get update
RUN apt-get install -y scala git curl openjdk-7-jdk
RUN git clone -b webPort https://github.com/Elyrixia/zipkin.git
RUN (cd zipkin; bin/sbt compile)