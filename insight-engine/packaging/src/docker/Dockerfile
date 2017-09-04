# Alfresco Solr Docker image
FROM openjdk:8u111-jdk-alpine

#ARG solrUrl
#ARG solrBranch
#ARG solrVer
#ENV SOLR_URL ${solrUrl:-https://nightlybuilds.alfresco.com/InsightEngine/$solrBranch/LATEST/Distribution/alfresco-insight-engine-distribution-$solrVer.zip}
ENV SOLR_URL https://nightlybuilds.alfresco.com/InsightEngine/develop/LATEST/Distribution/alfresco-insight-engine-distribution-1.0.0-SNAPSHOT.zip
ENV SOLR_ZIP alfresco-insight-engine.zip
ENV DIST_DIR /opt/alfresco-insight-engine
ENV LANG C.UTF-8

RUN addgroup solr && adduser -s /bin/bash -D -G solr solr
RUN set -x \
   && apk add --no-cache --virtual .fetch-deps \
      ca-certificates \
      vim unzip lsof \
      bash openssl wget \
   && wget --no-check-certificate -O "$SOLR_ZIP" "$SOLR_URL" \
   && unzip "$SOLR_ZIP" -d /opt/ && rm "$SOLR_ZIP"\
   && mkdir -p $DIST_DIR/data \
   && mv $DIST_DIR/solrhome/alfrescoModels $DIST_DIR/data/  \
   && chown -R solr:solr $DIST_DIR \
   && echo '#Docker Setup' >> $DIST_DIR/solr.in.sh \
    && echo 'SOLR_OPTS="$SOLR_OPTS -Dsolr.data.dir.root=/opt/alfresco-insight-engine/data -Dsolr.solr.model.dir=/opt/alfresco-insight-engine/data/alfrescoModels"' >> $DIST_DIR/solr.in.sh

WORKDIR $DIST_DIR

VOLUME /opt/alfresco-insight-engine/data
VOLUME /opt/alfresco-insight-engine/solrhome

EXPOSE 8983
USER solr
CMD [ "/opt/alfresco-insight-engine/solr/bin/solr", "start", "-f" ]
