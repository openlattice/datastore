FROM openjdk:8u102-jdk

EXPOSE 8080

RUN wget https://github.com/jwilder/dockerize/releases/download/v0.2.0/dockerize-linux-amd64-v0.2.0.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-v0.2.0.tar.gz

WORKDIR /datastoreBuild

COPY . ./

RUN git fetch --depth=10000

RUN ./gradlew distTar; cp src/main/resources/rhizome.yaml.prod /opt/rhizome.yaml; mv build/distributions/datastore.tgz /opt

RUN tar -xzvf /opt/datastore.tgz -C /opt \
  && cd /opt/datastore/lib \
  && mv /opt/rhizome.yaml ./rhizome.yaml \
  && DSVER=`ls | grep datastore` \
  && jar vfu $DSVER rhizome.yaml \
  && rm /opt/rhizome.yaml \
  && rm -rf /datastoreBuild

CMD dockerize -wait tcp://conductor:5701 -timeout 300s; /opt/datastore/bin/datastore cassandra
