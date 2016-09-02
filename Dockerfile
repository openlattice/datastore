FROM java:8u92-jre-alpine

RUN apk add --update bash wget && rm -rf /var/cache/apk/* \
    && wget https://github.com/jwilder/dockerize/releases/download/v0.2.0/dockerize-linux-amd64-v0.2.0.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-v0.2.0.tar.gz

ARG IMAGE_NAME
ARG IMG_VER

ENV VERSION=${IMG_VER:-v1.0.0} NAME=${IMAGE_NAME:-derpName}

ADD $NAME-$VERSION.tgz /opt

CMD dockerize -wait tcp://cassandra:9042 -timeout 300s; /opt/$NAME-$VERSION/bin/$NAME cassandra

EXPOSE 5701
