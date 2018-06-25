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
# limitations under the License.

FROM ubuntu:16.04
# Original Apache Zeppelin Docker Image has been copied and modified.
# This docker file will be maintained by Alfresco Software Ltd.

# `Z_VERSION` will be updated by `dev/change_zeppelin_version.sh`
ENV Z_VERSION="${apache.zeppelin.version}" \
    Z_VERSION_SUFFIX="alfresco-patched-2" \
    LOG_TAG="[ZEPPELIN_${Z_VERSION}]:" \
    Z_HOME="/zeppelin" \
    Z_PORT="9090" \
    Z_SAMPLE_NOTEBOOK="2CVRR91J2" \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

RUN echo "$LOG_TAG update and install basic packages" && \
    apt-get -y update && \
    apt-get install -y locales && \
    locale-gen $LANG && \
    apt-get install -y software-properties-common && \
    apt -y autoclean && \
    apt -y dist-upgrade && \
    apt-get install -y build-essential

RUN echo "$LOG_TAG install tini related packages" && \
    apt-get install -y wget curl grep sed dpkg && \
    TINI_VERSION=`curl https://github.com/krallin/tini/releases/latest | grep -o "/v.*\"" | sed 's:^..\(.*\).$:\1:'` && \
    curl -L "https://github.com/krallin/tini/releases/download/v${TINI_VERSION}/tini_${TINI_VERSION}.deb" > tini.deb && \
    dpkg -i tini.deb && \
    rm tini.deb

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
RUN echo "$LOG_TAG Install java8" && \
    apt-get -y update && \
    apt-get install -y openjdk-8-jdk && \
    rm -rf /var/lib/apt/lists/*

# should install conda first before numpy, matploylib since pip and python will be installed by conda
RUN echo "$LOG_TAG Install miniconda2 related packages" && \
    apt-get -y update && \
    apt-get install -y bzip2 ca-certificates \
    libglib2.0-0 libxext6 libsm6 libxrender1 \
    git mercurial subversion && \
    echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh && \
    wget --quiet https://repo.continuum.io/miniconda/Miniconda2-4.3.11-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh
ENV PATH /opt/conda/bin:$PATH

RUN echo "$LOG_TAG Install python related packages" && \
    apt-get -y update && \
    apt-get install -y python-dev python-pip && \
    apt-get install -y gfortran && \
    # numerical/algebra packages
    apt-get install -y libblas-dev libatlas-dev liblapack-dev && \
    # font, image for matplotlib
    apt-get install -y libpng-dev libfreetype6-dev libxft-dev && \
    # for tkinter
    apt-get install -y python-tk libxml2-dev libxslt-dev zlib1g-dev && \
    pip install numpy && \
    pip install matplotlib

RUN echo "$LOG_TAG Install R related packages" && \
    echo "deb http://cran.rstudio.com/bin/linux/ubuntu xenial/" | tee -a /etc/apt/sources.list && \
    gpg --keyserver keyserver.ubuntu.com --recv-key E084DAB9 && \
    gpg -a --export E084DAB9 | apt-key add - && \
    apt-get -y update && \
    apt-get -y install r-base r-base-dev && \
    R -e "install.packages('knitr', repos='http://cran.us.r-project.org')" && \
    R -e "install.packages('ggplot2', repos='http://cran.us.r-project.org')" && \
    R -e "install.packages('googleVis', repos='http://cran.us.r-project.org')" && \
    R -e "install.packages('data.table', repos='http://cran.us.r-project.org')" && \
    # for devtools, Rcpp
    apt-get -y install libcurl4-gnutls-dev libssl-dev && \
    R -e "install.packages('devtools', repos='http://cran.us.r-project.org')" && \
    R -e "install.packages('Rcpp', repos='http://cran.us.r-project.org')" && \
    Rscript -e "library('devtools'); library('Rcpp'); install_github('ramnathv/rCharts')"

RUN echo "$LOG_TAG Download Zeppelin binary" && \
    wget -O /tmp/zeppelin-${Z_VERSION}-${Z_VERSION_SUFFIX}.tar.gz http://nexus.alfresco.com/nexus/service/local/repositories/releases/content/org/apache/zeppelin/${Z_VERSION}-${Z_VERSION_SUFFIX}/zeppelin-${Z_VERSION}-${Z_VERSION_SUFFIX}.tar.gz && \
    tar -zxvf /tmp/zeppelin-${Z_VERSION}-${Z_VERSION_SUFFIX}.tar.gz && \
    rm -rf /tmp/zeppelin-${Z_VERSION}-${Z_VERSION_SUFFIX}.tar.gz && \
    mv /zeppelin-${Z_VERSION} ${Z_HOME}

RUN echo "$LOG_TAG Cleanup" && \
    apt-get autoclean && \
    apt-get clean

RUN echo "$LOG_TAG Backup original Zeppelin WAR file" && \
    cd ${Z_HOME} && \
    mv zeppelin-web-${Z_VERSION}.war zeppelin-web-${Z_VERSION}.war.back

RUN echo "$LOG_TAG Create zeppelin-evn file and change port number for Zeppelin" && \
    cd ${Z_HOME}/conf && \
    cp zeppelin-env.sh.template zeppelin-env.sh && \
    echo "\n# The default values for ZEPPELIN_NOTEBOOK_PUBLIC and ZEPPELIN_CREDENTIALS_PERSIST have been set to false programmatically" >> zeppelin-env.sh && \
    echo "\nexport ZEPPELIN_PORT=${Z_PORT}" >> zeppelin-env.sh && \
    echo "\nexport ZEPPELIN_SERVER_CONTEXT_PATH=/zeppelin" >> zeppelin-env.sh

RUN echo "$LOG_TAG Create folders for InsightJDBC driver and sample notebook" && \
    cd ${Z_HOME} && \
    mkdir alfresco && \
    cd ${Z_HOME}/notebook && \
    mkdir ${Z_SAMPLE_NOTEBOOK}

RUN echo "$LOG_TAG Install nano" && \
    apt-get -y update && \
    apt-get -y install nano

COPY ${project.artifactId}-${project.version}.war ${Z_HOME}/zeppelin-web-${Z_VERSION}.war
COPY note.json ${Z_HOME}/notebook/${Z_SAMPLE_NOTEBOOK}
COPY ${alfresco-insight-jdbc.name}-${alfresco-insight-jdbc.version}.jar ${Z_HOME}/alfresco
COPY interpreter.json ${Z_HOME}/conf
COPY notebook-authorization.json ${Z_HOME}/conf
COPY shiro.ini ${Z_HOME}/conf
COPY substituter.sh ${Z_HOME}/alfresco
RUN chmod +x ${Z_HOME}/alfresco/substituter.sh

EXPOSE ${Z_PORT}

ENTRYPOINT [ "/usr/bin/tini", "--" ]
WORKDIR ${Z_HOME}

CMD ${Z_HOME}/alfresco/substituter.sh bin/zeppelin.sh
