FROM apache/airflow:2.0.1

ARG USER_ID
ARG GROUP_ID

ENV AIRFLOW_HOME "/opt/airflow"
ENV JAVA_HOME "/usr/lib/jvm/java-11-openjdk-amd64"
ENV SPARK_HOME "/opt/spark"
ENV PATH "${PATH}:${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV SPARK_DIST_CLASSPATH "${SPARK_DIST_CLASSPATH}:${SPARK_HOME}/jars"

USER root

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
        | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - \
    && apt-get update -q=5 \
    && apt-get install --no-install-recommends -q=5 \
        default-jdk \
        unzip \
        wget \
        google-cloud-sdk \
    && apt-get autoremove -yqq --purge \
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

COPY tools ./tools

RUN usermod -u ${USER_ID} airflow \
    && groupmod -g ${GROUP_ID} airflow \
    && chown -R airflow ${AIRFLOW_HOME} \
    && chmod +x ./tools/*.sh

WORKDIR ${AIRFLOW_HOME}

USER airflow

COPY requirements.txt .

RUN pip install \
    --quiet \
    --no-cache-dir \
    --requirement requirements.txt

HEALTHCHECK CMD curl --fail http://localhost:8080 || exit 1
ENTRYPOINT [ "/bin/bash", "-c" ]
CMD [ "./tools/start.sh" ]
