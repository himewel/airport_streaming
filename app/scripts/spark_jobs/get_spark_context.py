from pyspark import SparkContext, SparkConf


gcp_credentials = "/credentials/gcloud_credentials.json"
hadoop_classpath = "/opt/gcs-connector-hadoop3-latest.jar"

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
    # ("spark.sql.parquet.mergeSchema", "true"),
]


def get_spark_context():
    conf = SparkConf()
    for key, value in settings_list:
        conf = conf.set(key, value)
    spark_context = SparkContext.getOrCreate(conf=conf)
    return spark_context
