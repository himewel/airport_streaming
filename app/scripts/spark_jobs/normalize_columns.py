import logging
from fnmatch import fnmatch

from pyspark.sql import functions as sf

_PARTITIONS = 1
_PREFIX_DTYPES = {
    "int": ["id"],
    "float": ["nr_", "lt_", "kg_", "km_"],
    "date": ["dt_"],
    "string": ["nm_", "sg_", "cd_", "hr_", "ds_"],
}


def get_column_dtype(column_name):
    column_dtype = ""
    for dtype, prefix_list in _PREFIX_DTYPES.items():
        check_dtype = [column_name.startswith(prefix) for prefix in prefix_list]
        if any(check_dtype):
            column_dtype = dtype
            break

    if not column_dtype:
        logging.info(f"dtype not found to {column_name}, casting as string")
        column_dtype = "string"

    return column_dtype


def remove_substring(new_name, allow_substrings):
    new_name_words = []
    for word in new_name.split("_"):
        check_words = [fnmatch(word, substring) for substring in allow_substrings]
        if not any(check_words):
            new_name_words.append(word)
    new_name = "_".join(new_name_words)
    return new_name


def expand_column_name(column_name):
    column_name = (
        column_name.replace("nm_", "nome_")
        .replace("ds_", "descricao_")
        .replace("nr_", "numero_")
        .replace("cd_", "codigo_")
        .replace("sg_", "sigla_")
        .replace("dt_", "data_")
        .replace("hr_", "hora_")
    )
    return column_name


def normalize_columns(df=None, allow_columns=[], allow_substrings=None):
    casting_list = []
    fillna_dict = {}

    for column_name in allow_columns:
        column_dtype = get_column_dtype(column_name)
        new_name = expand_column_name(column_name)
        new_name = new_name.strip("_")

        if allow_substrings:
            new_name = remove_substring(new_name, allow_substrings)

        if column_dtype == "float" or column_dtype == "int":
            fillna_dict[new_name] = 0
        elif column_dtype == "string":
            fillna_dict[new_name] = "NÃO INFORMADO"

        if column_dtype == "date":
            column_casting = sf.to_date(sf.col(column_name), "yyyy-MM-dd").alias(
                new_name
            )
        else:
            column_casting = sf.col(column_name).cast(column_dtype).alias(new_name)
        casting_list.append(column_casting)

    return df.coalesce(_PARTITIONS).select(*casting_list).fillna(fillna_dict)
