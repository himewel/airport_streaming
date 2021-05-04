FROM debian:buster-slim

ENV AIRFLOW_HOME "/opt/airflow"
ENV JAVA_HOME "/usr/lib/jvm/java-11-openjdk-amd64"
ENV SPARK_HOME "/opt/spark"
ENV PATH "${PATH}:${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV SPARK_DIST_CLASSPATH "${SPARK_DIST_CLASSPATH}:${SPARK_HOME}/jars"
ENV DEBIAN_FRONTEND "noninteractive"

USER root

RUN mkdir -p /usr/share/man/man1 \
    && apt-get update -q=5 \
    && apt-get install --no-install-recommends -q=5 \
        gcc \
        g++ \
        make \
        xz-utils \
        curl \
        default-jdk \
        unzip \
        wget \
# não sei desses de baixo
        zlib1g-dev \
        libncurses5-dev \
        liblzma-dev \
        libgdbm-dev \
        libnss3-dev \
        libssl-dev \
        libsqlite3-dev \
        libreadline-dev \
        libffi-dev \
        libbz2-dev \
        freetds-bin \
        krb5-user \
        ldap-utils \
        libffi6 \
        libsasl2-2 \
        libsasl2-modules \
        libssl1.1 \
        locales  \
        lsb-release \
        sasl2-bin \
        sqlite3 \
        unixodbc \
        gnupg \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf -- /var/lib/apt/lists/*

RUN curl -C - "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.0/gcs-connector-hadoop3-2.2.0-shaded.jar" \
        -o ./docker/gcs-connector-hadoop3-2.2.0-shaded.jar \
    && curl -C - "https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz" \
        -o ./docker/spark-3.0.1-bin-hadoop3.2.tgz \
    && curl -C - "https://www.python.org/ftp/python/3.8.7/Python-3.8.7.tar.xz" \
        -o ./docker/Python-3.8.7.tar.xz\
    && curl -C - "https://repo.mysql.com/mysql-apt-config_0.8.16-1_all.deb" \
        -o ./docker/mysql-apt-config_0.8.16-1_all.deb

RUN tar -xf Python-3.8.7.tar.xz \
    && cd Python-3.8.7 \
    && ./configure \
    && make -j $(nproc) -s \
    && make altinstall -s \
    && cd .. \
    && rm -rf Python-3.8.7.tar.xz Python-3.8.7 \
    && ln -s /usr/local/bin/python3.8 /usr/bin/python \
    && ln -s /usr/local/bin/pip3.8 /usr/bin/pip

RUN dpkg -i mysql-apt-config_0.8.16-1_all.deb \
    && apt-get update -qqq \
    && apt-get install --no-install-recommends -qqq \
        libmysqlclient21 \
        libmysqlclient-dev \
        mysql-client \
    && rm mysql-apt-config_0.8.16-1_all.deb \
    && apt-get autoremove -qqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV CONSTRAINT="https://raw.githubusercontent.com/apache/airflow/constraints-2.0.1/constraints-3.8.txt"

COPY requirements.txt .

RUN pip install \
    --quiet \
    --no-cache-dir \
    --requirement requirements.txt \
    --constraint ${CONSTRAINT}

RUN tar -xf spark-3.0.1-bin-hadoop3.2.tgz \
    && rm -rf spark-3.0.1-bin-hadoop3.2.tgz \
    && mv spark-3.0.1-bin-hadoop3.2 ${SPARK_HOME} \
    && mv gcs-connector-hadoop3-2.2.0-shaded.jar ${SPARK_HOME}/jars \
    && echo "log4j.appender.FILE.layout.conversionPattern=%m%n" >> ${SPARK_HOME}/conf/log4j.properties

WORKDIR ${AIRFLOW_HOME}
CMD airflow
