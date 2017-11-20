FROM ubuntu:16.04
# Original Apache Zeppelin Docker Image has been copied and modified.
# This docker file will be maintained by Alfresco Software Ltd.

# `Z_VERSION` will be updated by `dev/change_zeppelin_version.sh`
ENV Z_VERSION="${apache.zeppelin.version}" \
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
    wget -O /tmp/zeppelin-${Z_VERSION}-bin-all.tgz http://archive.apache.org/dist/zeppelin/zeppelin-${Z_VERSION}/zeppelin-${Z_VERSION}-bin-all.tgz && \
    tar -zxvf /tmp/zeppelin-${Z_VERSION}-bin-all.tgz && \
    rm -rf /tmp/zeppelin-${Z_VERSION}-bin-all.tgz && \
    mv /zeppelin-${Z_VERSION}-bin-all ${Z_HOME}

RUN echo "$LOG_TAG Cleanup" && \
    apt-get autoclean && \
    apt-get clean

RUN echo "$LOG_TAG Backup original Zeppelin WAR file" && \
    cd ${Z_HOME} && \
    mv zeppelin-web-${Z_VERSION}.war zeppelin-web-${Z_VERSION}.war.back

RUN echo "$LOG_TAG Create zeppelin-evn file and change port number for Zeppelin" && \
    cd ${Z_HOME}/conf && \
    cp zeppelin-env.sh.template zeppelin-env.sh && \
    echo "\nexport ZEPPELIN_PORT=${Z_PORT}" >> zeppelin-env.sh && \
    echo "\nexport ZEPPELIN_NOTEBOOK_PUBLIC="false" >> zeppelin-env.sh"

RUN echo "$LOG_TAG Create shiro.ini file" && \
    cd ${Z_HOME}/conf && \
    cp shiro.ini.template shiro.ini && \
    sed -i -e 's/admin = password1, admin/admin = admin, admin/g' \
    -e 's/#\/api\/interpreter\/\*\* = authc, roles\[admin\]/\/api\/interpreter\/\*\* = authc, roles\[admin\]/g' \
    -e 's/#\/api\/configurations\/\*\* = authc, roles\[admin\]/\/api\/configurations\/\*\* = authc, roles\[admin\]/g' \
    -e 's/#\/api\/credential\/\*\* = authc, roles\[admin\]/\/api\/credential\/\*\* = authc, roles\[admin\]/g' \
    -e 's/user1 = password2, role1, role2/user1 = user1/g' \
    -e 's/user2 = password3, role3/user2 = user2/g' \
    -e 's/user3 = password4, role2/user3 = user3/g' shiro.ini

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

EXPOSE ${Z_PORT}

ENTRYPOINT [ "/usr/bin/tini", "--" ]
WORKDIR ${Z_HOME}

CMD ["bin/zeppelin.sh"]
