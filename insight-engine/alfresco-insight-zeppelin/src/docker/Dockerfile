FROM quay.io/alfresco/alfresco-base-java:8u161-oracle-centos-7-ce1a67232405

ENV ZEPPELIN_DISTRIBUTION=${project.build.finalName}.zip \
    ZEPPELIN_HOME="/zeppelin" \
    LOG="[ZEPPELIN]:" \
    TINI_VERSION=v0.18.0

ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN echo "$LOG Install packages" && \
    yum update -y && \
    yum install -y unzip && \
    yum install -y nano && \
    yum clean all

COPY ${ZEPPELIN_DISTRIBUTION} .
RUN echo "$LOG Extract Zeppelin" && \
    unzip ${ZEPPELIN_DISTRIBUTION}

RUN echo "$LOG Update substituter.sh to start zeppelin.sh" && \
    echo " " >> ${ZEPPELIN_HOME}/bin/substituter.sh && \
    echo "bash -c \"\$@\"" >> ${ZEPPELIN_HOME}/bin/substituter.sh

EXPOSE ${ZEPPELIN_PORT}

ENTRYPOINT [ "/tini", "--" ]
WORKDIR ${ZEPPELIN_HOME}

CMD ${ZEPPELIN_HOME}/bin/substituter.sh ${ZEPPELIN_HOME}/bin/zeppelin.sh
