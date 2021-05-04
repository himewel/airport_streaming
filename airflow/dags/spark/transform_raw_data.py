# This scripts wrap the normalization for tables created directly from the
# raw data
#
# Expected arguments:
#   - raw_source: path where raw data is stored in gcs
#   - storage_filepath: filepath in storage to fill the output
#   - table_name: table to process

import logging
import os
from sys import argv

from get_schema import get_allow_substrings, get_column_names, get_raw_schema
from get_spark_context import get_spark_context
from normalize_columns import normalize_columns
from pyspark.sql import SQLContext

logging.basicConfig(level=logging.INFO)
_PARTITIONS = 1
bucket_name = os.getenv("GCS_BUCKET")

raw_source = str(argv[1])
storage_filepath = str(argv[2])
table_name = str(argv[3])

logging.info(raw_source)
logging.info(storage_filepath)
logging.info(table_name)

spark_context = get_spark_context()
sql_context = SQLContext(spark_context)

current_df = sql_context.readStream.csv(
    raw_source,
    header="true",
    sep=";",
    encoding="ISO-8859-1",
    schema=get_raw_schema(),
)

# get columns matching with substrings
columns_to_select = get_column_names(table_name)

# filter columns
current_df = current_df.select(columns_to_select)
current_df.printSchema()

# rename columns and fix dtypws
current_df = normalize_columns(
    df=current_df,
    allow_columns=columns_to_select,
    allow_substrings=get_allow_substrings(table_name),
)
current_df.printSchema()

query = (
    current_df.drop_duplicates(["id"])
    .writeStream.trigger(processingTime="1 minute")
    .start(
        f"{storage_filepath}/{table_name}/",
        header="true",
        format="csv",
        checkpointLocation=f"gs://{bucket_name}/checkpoint/{table_name}",
        mode="complete",
        failOnDataLoss="false",
    )
)

query.awaitTermination()
