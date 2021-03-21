# This scripts wrap the normalization for tables created directly from the
# raw data
#
# Expected arguments:
#   - raw_source: path where raw data is stored in gcs
#   - storage_filepath: filepath in storage to fill the output

import logging
from sys import argv

import reverse_geocoder as rg
from geopy.geocoders import Nominatim
from get_schema import get_allow_substrings, get_column_names, get_raw_schema
from get_spark_context import get_spark_context
from normalize_columns import normalize_columns
from pyspark.sql import SQLContext
from pyspark.sql import functions as sf

logging.basicConfig(level=logging.INFO)
_PARTITIONS = 1

raw_source = str(argv[1])
storage_filepath = str(argv[2])

logging.info(raw_source)
logging.info(storage_filepath)

spark_context = get_spark_context()
sql_context = SQLContext(spark_context)

raw_df = sql_context.readStream.csv(
    raw_source,
    header="true",
    sep=";",
    encoding="ISO-8859-1",
    schema=get_raw_schema(),
)

# filter columns
origem_df = normalize_columns(
    df=raw_df,
    allow_columns=get_column_names("dim_aerodromos_origem"),
    allow_substrings=get_allow_substrings("dim_aerodromos_origem"),
)
destino_df = normalize_columns(
    df=raw_df,
    allow_columns=get_column_names("dim_aerodromos_destino"),
    allow_substrings=get_allow_substrings("dim_aerodromos_origem"),
)


@sf.udf("string")
def get_coordinate(search, type):
    geolocator = Nominatim(
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/51.0.2704.103 Safari/537.36"
        ),
        timeout=None,
    )
    location = geolocator.geocode(search)

    try:
        coordinate = location.raw[type]
    except Exception:
        coordinate = None
    return coordinate


@sf.udf("string")
def get_alpha2code(latitude, longitude):
    try:
        res = rg.search((latitude, longitude))
        alpha_code = res[0]["cc"]
    except Exception:
        alpha_code = ""

    return alpha_code


# concat dataframes, drop duplicates and get geo coordinates
dim_aerodromos = (
    origem_df.union(destino_df)
    .drop_duplicates(["id"])
    .withColumn(
        "search",
        sf.concat(
            sf.col("nome_municipio"),
            sf.lit(", "),
            sf.col("sigla_uf"),
            sf.lit(" - "),
            sf.col("nome_pais"),
        ),
    )
    .withColumn("latitude", get_coordinate("search", sf.lit("lat")).cast("float"))
    .withColumn("longitude", get_coordinate("search", sf.lit("lon")).cast("float"))
    .withColumn("alpha2code", get_alpha2code("latitude", "longitude"))
    .drop("search")
)
dim_aerodromos.printSchema()

query = (
    dim_aerodromos.drop_duplicates(["id"])
    .writeStream.trigger(processingTime="1 minute")
    .start(
        f"{storage_filepath}/dim_aerodromos/",
        header="true",
        format="csv",
        checkpointLocation="gs://anac_data_lake/checkpoint/dim_aerodromos",
        mode="complete",
        failOnDataLoss="false",
    )
)

query.awaitTermination()
