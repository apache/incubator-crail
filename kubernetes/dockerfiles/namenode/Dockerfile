FROM ubuntu:16.04

# set environment vars
ENV CRAIL_HOME /opt/crail/
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# install packages
RUN \
  apt-get update && apt-get install -y \
  ssh \
  rsync \
  vim \
  openjdk-8-jdk \
  net-tools iputils-ping

WORKDIR /opt/crail
COPY crail-apache/  /opt/crail/

# Start namenode
CMD bash start_namenode.sh
#CMD bash while_loop.sh
