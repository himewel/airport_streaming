FROM python:3.8-buster

ENV JAVA_HOME "/usr/lib/jvm/java-11-openjdk-amd64"
ENV SPARK_HOME "/opt/spark"
ENV PATH "${PATH}:${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV SPARK_DIST_CLASSPATH "${SPARK_DIST_CLASSPATH}:${SPARK_HOME}/jars"
ENV SPARK_NO_DAEMONIZE TRUE

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
        | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - \
    && mkdir -p /usr/share/man/man1 \
    && apt-get update -q=5 \
    && apt-get install --no-install-recommends -q=5 \
        default-jdk \
        google-cloud-sdk \
    && apt-get remove --purge -q=5 ${builds_deps} \
    && apt-get clean \
    && rm -rf -- /var/lib/apt/lists/*

RUN curl -C - "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.0/gcs-connector-hadoop3-2.2.0-shaded.jar" \
        -o ./gcs-connector-hadoop3-2.2.0-shaded.jar
RUN curl -C - "https://downloads.apache.org/spark/spark-3.0.2/spark-3.0.2-bin-hadoop3.2.tgz" \
        -o ./spark-3.0.2-bin-hadoop3.2.tgz

RUN tar -xf spark-3.0.2-bin-hadoop3.2.tgz \
    && rm -rf spark-3.0.2-bin-hadoop3.2.tgz \
    && mv spark-3.0.2-bin-hadoop3.2 ${SPARK_HOME} \
    && mv gcs-connector-hadoop3-2.2.0-shaded.jar ${SPARK_HOME}/jars \
    && echo "log4j.appender.FILE.layout.conversionPattern=%m%n" >> ${SPARK_HOME}/conf/log4j.properties

COPY requirements.txt .

RUN pip3 install \
    --quiet \
    --no-cache-dir \
    --requirement requirements.txt

WORKDIR ${SPARK_HOME}

ENTRYPOINT [ "/bin/bash" ]
CMD [ "spark-master.sh" ]
