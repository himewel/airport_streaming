import json
import logging
import time
from sys import argv

from pyspark.sql import SQLContext, functions as sf

from get_schema import get_raw_schema, get_schema, get_column_names
from get_spark_context import get_spark_context
from normalize_columns import normalize_columns, expand_column_name


logging.basicConfig(level=logging.INFO)
_PARTITIONS = 1

raw_source = str(argv[1])
storage_filepath = str(argv[2])
key_mapping = json.loads(str(argv[3]))

spark_context = get_spark_context()
sql_context = SQLContext(spark_context)

logging.info("Collected variables")
logging.info(raw_source)
logging.info(storage_filepath)
logging.info(key_mapping)

# get raw data
raw_df = sql_context.readStream.csv(
    raw_source,
    header="true",
    sep=";",
    encoding="ISO-8859-1",
    schema=get_raw_schema(),
).alias("raw_df")
raw_df.createOrReplaceTempView("raw_df")

# rename and cast columns
raw_df = normalize_columns(df=raw_df, allow_columns=raw_df.schema.names)
raw_df = raw_df.alias("raw_df")
raw_df.createOrReplaceTempView("raw_df")
raw_df.printSchema()

df_columns = raw_df.schema.names
key_list = []
drop_columns = []
alias_list = []
join_mappings = []

# build join cells
for table_name, mapping_list in key_mapping.items():
    logging.info(f"Mapping table {table_name}")
    schema = get_schema(table_name=table_name, normalize=True)

    while True:
        try:
            table_df = sql_context.readStream.schema(schema).csv(
                f"gs://anac_data_lake/dw/{table_name}/*.csv",
                schema=schema,
            )
            break
        except Exception as e:
            logging.exception(e)
            logging.warning("Retrying in 5 seconds...")
            time.sleep(5)

    for key, mapping in mapping_list.items():
        left = mapping.split(":")[0]
        right = mapping.split(":")[1]
        key_list.append(key)
        drop_columns.append(left)
        alias_list.append(f"{left}_to_{right}.id as {key}")

        logging.info(f"{key} is raw_df.{left} on {table_name}.{right}")
        table_df = table_df.alias(f"{left}_to_{right}")
        table_df.createOrReplaceTempView(f"{left}_to_{right}")

        raw_df = raw_df.coalesce(_PARTITIONS).join(
            table_df,
            sf.col(f"raw_df.{left}") == sf.col(f"{left}_to_{right}.{right}"),
        )

# make join
df_columns = [f"raw_df.{column}" for column in df_columns]
column_as = ",".join(alias_list + df_columns)
raw_df = raw_df.coalesce(_PARTITIONS).selectExpr(*(alias_list + df_columns))
raw_df.createOrReplaceTempView("raw_df")
raw_df.printSchema()

# filter columns
allow_columns = key_list + get_column_names("fact_aircraft_moviments")
allow_columns = [expand_column_name(column) for column in allow_columns]
current_df = raw_df.coalesce(_PARTITIONS).select(*allow_columns)

# write file in storage
query = (
    current_df.coalesce(_PARTITIONS)
    .writeStream.trigger(processingTime="1 minute")
    .start(
        storage_filepath,
        header="true",
        format="csv",
        mode="append",
        checkpointLocation="gs://anac_data_lake/checkpoint/fact_aircraft_moviments",
        failOnDataLoss="false",
    )
)

query.awaitTermination()
