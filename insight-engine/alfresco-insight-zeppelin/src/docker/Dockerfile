FROM alfresco/alfresco-base-java:11.0.1-openjdk-centos-7-1fd3c4475374

ENV ZEPPELIN_DISTRIBUTION=${project.build.finalName}.zip \
    ZEPPELIN_HOME="/zeppelin" \
    LOG="[ZEPPELIN]:" \
    TINI_VERSION=v0.18.0

ARG USERNAME=zeppelin
ARG USERID=33007

ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN echo "$LOG Install packages" && \
    yum update -y && \
    yum install -y unzip && \
    yum install -y nano && \
    yum clean all

RUN set -x \
   && useradd \
        -c "Alfresco ${USERNAME}" \
        -M \
        -s "/bin/bash" \
        -u "${USERID}" \
        -o \
        "${USERNAME}"

COPY ${ZEPPELIN_DISTRIBUTION} .
RUN echo "$LOG Extract Zeppelin" && \
    unzip ${ZEPPELIN_DISTRIBUTION} && \
    chown -R ${USERNAME}:${USERNAME} $ZEPPELIN_HOME

RUN echo "$LOG Update substituter.sh to start zeppelin.sh" && \
    echo " " >> ${ZEPPELIN_HOME}/bin/substituter.sh && \
    echo "bash -c \"\$@\"" >> ${ZEPPELIN_HOME}/bin/substituter.sh

EXPOSE ${zeppelin.port}
USER ${USERNAME}

ENTRYPOINT [ "/tini", "--" ]
WORKDIR ${ZEPPELIN_HOME}

CMD ${ZEPPELIN_HOME}/bin/substituter.sh ${ZEPPELIN_HOME}/bin/zeppelin.sh
