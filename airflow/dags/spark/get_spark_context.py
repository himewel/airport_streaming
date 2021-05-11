import os

from pyspark import SparkConf, SparkContext

home = os.getenv("HOME")
default_credentials = f"{home}/.config/gcloud/application_default_credentials.json"
gcp_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", default_credentials)

spark_home = os.getenv("SPARK_HOME")
hadoop_classpath = f"{spark_home}/jars/gcs-connector-hadoop3-2.2.0-shaded.jar"

settings_list = [
    ("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"),
    (
        "fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    ),
    ("fs.gs.auth.service.account.enable", "true"),
    ("fs.gs.auth.service.account.json.keyfile", gcp_credentials),
    ("spark.driver.extraClassPath", hadoop_classpath),
    ("spark.executor.extraClassPath", hadoop_classpath),
    ("spark.sql.shuffle.partitions", "10"),
    ("spark.sql.legacy.timeParserPolicy", "LEGACY"),
]


def get_spark_context():
    conf = SparkConf()
    for key, value in settings_list:
        conf = conf.set(key, value)
    spark_context = SparkContext.getOrCreate(conf=conf)
    return spark_context
