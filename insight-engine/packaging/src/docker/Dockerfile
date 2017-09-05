# Alfresco Insight Engine Docker image
FROM openjdk:8u111-jdk-alpine

ARG url
ARG branch
ARG version
ENV URL ${url:-https://nightlybuilds.alfresco.com/InsightEngine/$branch/LATEST/Distribution/alfresco-insight-engine-distribution-$version.zip}
ENV ZIP alfresco-insight-engine.zip
ENV DIST_DIR /opt/alfresco-insight-engine
ENV LANG C.UTF-8

RUN addgroup solr && adduser -s /bin/bash -D -G solr solr
RUN set -x \
   && apk add --no-cache --virtual .fetch-deps \
      ca-certificates \
      vim unzip lsof \
      bash openssl wget \
   && wget --no-check-certificate -O "$ZIP" "$URL" \
   && unzip "$ZIP" -d /opt/ && rm "$ZIP"\
   && mkdir -p $DIST_DIR/data \
   && mv $DIST_DIR/solrhome/alfrescoModels $DIST_DIR/data/  \
   && chown -R solr:solr $DIST_DIR \
   && echo '#Docker Setup' >> $DIST_DIR/solr.in.sh \
    && echo 'SOLR_OPTS="$SOLR_OPTS -Dsolr.data.dir.root=$DIST_DIR/data -Dsolr.solr.model.dir=$DIST_DIR/data/alfrescoModels"' >> $DIST_DIR/solr.in.sh

WORKDIR $DIST_DIR

VOLUME $DIST_DIR/data
VOLUME $DIST_DIR/solrhome

EXPOSE 8983
USER solr
CMD $DIST_DIR/solr/bin/solr start -f
