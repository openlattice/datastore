FROM openjdk:8u102-jdk

RUN apt-get install wget curl bash \
    && wget https://github.com/jwilder/dockerize/releases/download/v0.2.0/dockerize-linux-amd64-v0.2.0.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-v0.2.0.tar.gz

WORKDIR /datastoreBuild

COPY . ./

RUN ./gradlew distTar; cp src/main/resources/rhizome.yaml.prod /opt/rhizome.yaml

ADD datastore.tgz /opt

RUN cd /opt/datastore/lib \
  && mv /opt/rhizome.yaml ./rhizome.yaml \
  && jar vfu datastore.jar rhizome.yaml \
  && rm /opt/rhizome.yaml*

EXPOSE 8080

CMD dockerize -wait tcp://conductor:5701 -timeout 300s; /opt/datastore/bin/datastore cassandra
