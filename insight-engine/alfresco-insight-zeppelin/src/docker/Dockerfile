FROM quay.io/alfresco/alfresco-base-java:8u161-oracle-centos-7-ce1a67232405

ENV ZEPPELIN_VERSION="${apache.zeppelin.version}" \
    ZEPPELIN_VERSION_SUFFIX="${alfresco.zeppelin.version.suffix}" \
    ZEPPELIN_DISTRIBUTION=${project.build.finalName}.zip \
    ZEPPELIN_HOME="/zeppelin" \
    ZEPPELIN_SERVER_CONTEXT_PATH="/zeppelin" \
    ZEPPELIN_PORT="9090" \
    ZEPPELIN_SAMPLE_NOTEBOOK_1="2CVRR91J2" \
    ZEPPELIN_SAMPLE_NOTEBOOK_2="2DMP8WY2G" \
    LOG="[ZEPPELIN_${ZEPPELIN_VERSION}]:" \
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

RUN echo "$LOG Create zeppelin-evn file and change port number and server context for Zeppelin" && \
    cd ${ZEPPELIN_HOME}/conf && \
    cp zeppelin-env.sh.template zeppelin-env.sh && \
    echo "# The default values for ZEPPELIN_NOTEBOOK_PUBLIC and ZEPPELIN_CREDENTIALS_PERSIST have been set to false programmatically" >> zeppelin-env.sh && \
    echo "export ZEPPELIN_PORT=${ZEPPELIN_PORT}" >> zeppelin-env.sh && \
    echo "export ZEPPELIN_SERVER_CONTEXT_PATH=${ZEPPELIN_SERVER_CONTEXT_PATH}" >> zeppelin-env.sh

RUN echo "$LOG Create folders for InsightJDBC driver and sample notebooks" && \
    cd ${ZEPPELIN_HOME} && \
    mkdir alfresco && \
    rm -r ${ZEPPELIN_HOME}/notebook/* && \
    cd ${ZEPPELIN_HOME}/notebook && \
    mkdir ${ZEPPELIN_SAMPLE_NOTEBOOK_1} && \
    mkdir ${ZEPPELIN_SAMPLE_NOTEBOOK_2} 

COPY note_1.json ${ZEPPELIN_HOME}/notebook/${ZEPPELIN_SAMPLE_NOTEBOOK_1}/note.json
COPY note_2.json ${ZEPPELIN_HOME}/notebook/${ZEPPELIN_SAMPLE_NOTEBOOK_2}/note.json
COPY ${alfresco-insight-jdbc.name}-${alfresco-insight-jdbc.version}.jar ${ZEPPELIN_HOME}/alfresco
COPY interpreter.json ${ZEPPELIN_HOME}/conf
COPY notebook-authorization.json ${ZEPPELIN_HOME}/conf
COPY shiro.ini ${ZEPPELIN_HOME}/conf
COPY substituter.sh ${ZEPPELIN_HOME}/alfresco
RUN chmod +x ${ZEPPELIN_HOME}/alfresco/substituter.sh

EXPOSE ${ZEPPELIN_PORT}

ENTRYPOINT [ "/tini", "--" ]
WORKDIR ${ZEPPELIN_HOME}

CMD ${ZEPPELIN_HOME}/alfresco/substituter.sh bin/zeppelin.sh
