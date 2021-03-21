#!/usr/bin/env bash

curl -C - "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.0/gcs-connector-hadoop3-2.2.0-shaded.jar" \
    -o ./docker/gcs-connector-hadoop3-2.2.0-shaded.jar

curl -C - "https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz" \
    -o ./docker/spark-3.0.1-bin-hadoop3.2.tgz

curl -C - "https://www.python.org/ftp/python/3.8.7/Python-3.8.7.tar.xz" \
    -o ./docker/Python-3.8.7.tar.xz

curl -C - "https://repo.mysql.com/mysql-apt-config_0.8.16-1_all.deb" \
    -o ./docker/mysql-apt-config_0.8.16-1_all.deb
