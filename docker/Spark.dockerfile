FROM python:3.8-slim-buster

ENV JAVA_HOME "/usr/lib/jvm/java-11-openjdk-amd64"
ENV SPARK_HOME "/opt/spark"
ENV PATH "${PATH}:${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV SPARK_DIST_CLASSPATH "${SPARK_DIST_CLASSPATH}:${SPARK_HOME}/jars"
ENV SPARK_NO_DAEMONIZE TRUE

RUN mkdir -p /usr/share/man/man1 \
    && apt-get update -q=5 \
    && apt-get install --no-install-recommends -q=5 default-jdk \
    && apt-get remove --purge -q=5 ${builds_deps} \
    && apt-get clean \
    && rm -rf -- /var/lib/apt/lists/*

COPY spark_requirements.txt .

RUN pip3 install \
    --quiet \
    --no-cache-dir \
    --requirement requirements.txt

COPY spark-3.0.1-bin-hadoop3.2.tgz .
COPY gcs-connector-hadoop3-2.2.0-shaded.jar .

RUN tar -xf spark-3.0.1-bin-hadoop3.2.tgz \
    && mv spark-3.0.1-bin-hadoop3.2 ${SPARK_HOME} \
    && rm -rf spark-3.0.1-bin-hadoop3.2.tgz \
    && mv gcs-connector-hadoop3-2.2.0-shaded.jar ${SPARK_HOME}/jars

WORKDIR ${SPARK_HOME}

CMD spark-master.sh
