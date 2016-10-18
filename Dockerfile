FROM openjdk:8u102-jdk

RUN apt-get install wget curl bash \
#    && curl -s https://get.sdkman.io | bash \
#    && source "/root/.sdkman/bin/sdkman-init.sh" \
#    && sdk install gradle 2.14 \
    && wget https://github.com/jwilder/dockerize/releases/download/v0.2.0/dockerize-linux-amd64-v0.2.0.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-v0.2.0.tar.gz

WORKDIR /datastoreBuild

COPY . ./
RUN ls -la
RUN git describe --tags --dirty --long

RUN ./gradlew tasks
RUN ./gradlew distTar; cp src/main/resources/rhizome.yaml.prod /opt/rhizome.yaml

ADD datastore-$VERSION.tgz /opt

RUN cd /opt/datastore-$VERSION/lib \
  && mv /opt/rhizome.yaml ./rhizome.yaml \
  && jar vfu datastore-$VERSION.jar rhizome.yaml \
  && rm /opt/rhizome.yaml*

EXPOSE 8080

CMD dockerize -wait tcp://conductor:5701 -timeout 300s; /opt/datastore-$VERSION/bin/datastore cassandra
