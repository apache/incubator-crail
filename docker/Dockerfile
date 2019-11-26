# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.)

FROM ubuntu:18.04
MAINTAINER Apache Crail <dev@crail.apache.org>

# TODO: automate update version on new release
ARG GIT_COMMIT="v1.2"
ENV LOG_COMMIT=$GIT_COMMIT

RUN echo "Crail-$LOG_COMMIT install openjdk8, git and envsubst" && \
    apt-get update && apt-get install --no-install-recommends -y \
    openjdk-8-jdk-headless \
    git \
    gettext-base \
    libxml2-utils
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=${PATH}:${JAVA_HOME}/bin

# We need to install maven separately since it otherwise pulls in java 10
RUN echo "Crail-$LOG_COMMIT install maven" && \
    apt-get install --no-install-recommends -y maven


RUN echo "Crail-$LOG_COMMIT clone & build Crail repo" && \
    git clone https://github.com/apache/incubator-crail.git && \
    cd incubator-crail && \
    git checkout ${GIT_COMMIT} && \
    mvn -DskipTests package

RUN echo "Move crail to /crail" && \
    rm -rf /crail && \
    v=`xmllint --xpath "string(/*[local-name()='project']/*[local-name()='version'])" /incubator-crail/pom.xml` && \
    mv /incubator-crail/assembly/target/apache-crail-${v}-bin/apache-crail-${v} /crail

ENV CRAIL_HOME /crail
ENV PATH=${PATH}:${CRAIL_HOME}/bin


ENV NAMENODE_HOST=localhost
ENV NAMENODE_PORT=9060
ENV INTERFACE=eth0
ENV DATAPATH=/dev/hugepages/data
ENV STORAGELIMIT=1073741824
ENV CACHEPATH=/dev/hugepages/cache
ENV CACHELIMIT=0

COPY ./start-crail-docker.sh $CRAIL_HOME/bin
COPY ./crail-env.sh $CRAIL_HOME/conf
COPY ./core-site.xml.env $CRAIL_HOME/conf
COPY ./crail-site.conf $CRAIL_HOME/conf

ENTRYPOINT ["start-crail-docker.sh"]
