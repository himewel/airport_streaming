# This script has the objective of generate a table with dates and some info
# related to the dates as halfyear name, quarter year name, etc.
#
# Expected arguments:
#   - storage_filepath: filepath in storage to fill the output
#   - start_date: start of the range of dates to be generated
#   - end_date: end of the range of dates to be generated

import logging
from sys import argv

from pyspark.sql import Window, SQLContext, functions as sf

from get_spark_context import get_spark_context
from normalize_columns import normalize_columns


logging.basicConfig(level=logging.INFO)
_PARTITIONS = 1

storage_filepath = str(argv[2])
start_date = str(argv[-2])
end_date = str(argv[-1])

spark_context = get_spark_context()
sql_context = SQLContext.getOrCreate(spark_context)

try:
    old_df = sql_context.read.csv(storage_filepath)
    logging.info("Dim dates is already there")
    quit()
except Exception:
    logging.info("Creating dim dates")

dates_df = sql_context.sql(
    f"""
    SELECT sequence(
        to_date('{start_date}'),
        to_date('{end_date}'),
        interval 1 day
    ) as dt_
    """
).withColumn("dt_", sf.explode(sf.col("dt_")))

dates_df = dates_df.coalesce(_PARTITIONS).select(
    "dt_",
    sf.year("dt_").alias("nr_ano"),
    sf.quarter("dt_").alias("nr_trimestre"),
    sf.month("dt_").alias("nr_mes"),
    sf.dayofweek("dt_").alias("nr_semana"),
    sf.dayofmonth("dt_").alias("nr_dia"),
)

dates_df = (
    dates_df.coalesce(_PARTITIONS)
    .withColumn(
        "nr_semestre",
        sf.when(dates_df.nr_trimestre <= 2, sf.lit(1)).otherwise(sf.lit(2)),
    )
    .withColumn("nm_dia_semana", sf.date_format(sf.col("dt_"), "EEEE"))
    .withColumn("nm_mes", sf.date_format(sf.col("dt_"), "MMMM"))
    .withColumn("nm_semestre", sf.concat(sf.col("nr_semestre"), sf.lit("º half")))
    .withColumn(
        "nm_trimestre",
        sf.concat(sf.col("nr_trimestre"), sf.lit("º quarter")),
    )
    .withColumn("nr_ano_mes", sf.concat(sf.col("nr_ano"), sf.col("nr_mes")))
)

dates_df = normalize_columns(df=dates_df, allow_columns=dates_df.schema.names)
dates_df = dates_df.alias("dates_df")
dates_df.createOrReplaceTempView("dates_df")

window = Window.orderBy(sf.col("data"))
dates_df = (
    dates_df.coalesce(_PARTITIONS)
    .withColumn("id", sf.row_number().over(window))
    .select(
        *[
            "data",
            "numero_ano",
            "numero_semestre",
            "nome_semestre",
            "numero_trimestre",
            "nome_trimestre",
            "numero_mes",
            "nome_mes",
            "numero_semana",
            "nome_dia_semana",
            "numero_dia",
            "numero_ano_mes",
            "id",
        ]
    )
)

dates_df.show()
dates_df.printSchema()

dates_df.coalesce(_PARTITIONS).write.save(
    f"{storage_filepath}/dim_dates",
    header="true",
    format="csv",
    mode="overwrite",
)
