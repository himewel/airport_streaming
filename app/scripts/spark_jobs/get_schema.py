import fnmatch
import logging

from pyspark.sql.types import (
    StringType,
    FloatType,
    IntegerType,
    DateType,
    StructType,
    StructField,
)

from normalize_columns import (
    get_column_dtype,
    remove_substring,
    expand_column_name,
)


_COLUMN_LIST = [
    "id_basica",
    "id_empresa",
    "sg_empresa_icao",
    "sg_empresa_iata",
    "nm_empresa",
    "nm_pais",
    "ds_tipo_empresa",
    "nr_voo",
    "nr_singular",
    "id_di",
    "cd_di",
    "ds_di",
    "ds_grupo_di",
    "dt_referencia",
    "nr_ano_referencia",
    "nr_semestre_referencia",
    "nm_semestre_referencia",
    "nr_trimestre_referencia",
    "nm_trimestre_referencia",
    "nr_mes_referencia",
    "nm_mes_referencia",
    "nr_semana_referencia",
    "nm_dia_semana_referencia",
    "nr_dia_referencia",
    "nr_ano_mes_referencia",
    "id_tipo_linha",
    "cd_tipo_linha",
    "ds_tipo_linha",
    "ds_natureza_tipo_linha",
    "ds_servico_tipo_linha",
    "ds_natureza_etapa",
    "hr_partida_real",
    "dt_partida_real",
    "nr_ano_partida_real",
    "nr_semestre_partida_real",
    "nm_semestre_partida_real",
    "nr_trimestre_partida_real",
    "nm_trimestre_partida_real",
    "nr_mes_partida_real",
    "nm_mes_partida_real",
    "nr_semana_partida_real",
    "nm_dia_semana_partida_real",
    "nr_dia_partida_real",
    "nr_ano_mes_partida_real",
    "id_aerodromo_origem",
    "sg_icao_origem",
    "sg_iata_origem",
    "nm_aerodromo_origem",
    "nm_municipio_origem",
    "sg_uf_origem",
    "nm_regiao_origem",
    "nm_pais_origem",
    "nm_continente_origem",
    "nr_etapa",
    "hr_chegada_real",
    "dt_chegada_real",
    "nr_ano_chegada_real",
    "nr_semestre_chegada_real",
    "nm_semestre_chegada_real",
    "nr_trimestre_chegada_real",
    "nm_trimestre_chegada_real",
    "nr_mes_chegada_real",
    "nm_mes_chegada_real",
    "nr_semana_chegada_real",
    "nm_dia_semana_chegada_real",
    "nr_dia_chegada_real",
    "nr_ano_mes_chegada_real",
    "id_equipamento",
    "sg_equipamento_icao",
    "ds_modelo",
    "ds_matricula",
    "id_aerodromo_destino",
    "sg_icao_destino",
    "sg_iata_destino",
    "nm_aerodromo_destino",
    "nm_municipio_destino",
    "sg_uf_destino",
    "nm_regiao_destino",
    "nm_pais_destino",
    "nm_continente_destino",
    "nr_escala_destino",
    "lt_combustivel",
    "nr_assentos_ofertados",
    "kg_payload",
    "km_distancia",
    "nr_passag_pagos",
    "nr_passag_gratis",
    "kg_bagagem_livre",
    "kg_bagagem_excesso",
    "kg_carga_paga",
    "kg_carga_gratis",
    "kg_correio",
    "nr_decolagem",
    "nr_horas_voadas",
    "kg_peso",
    "nr_velocidade_media",
    "nr_pax_gratis_km",
    "nr_carga_paga_km",
    "nr_carga_gratis_km",
    "nr_correio_km",
    "nr_bagagem_paga_km",
    "nr_bagagem_gratis_km",
    "nr_ask",
    "nr_rpk",
    "nr_atk",
    "nr_rtk",
    "id_arquivo",
    "nm_arquivo",
    "nr_linha",
    "dt_sistema",
]


_COLUMN_PATTERNS = {
    "dim_dates": {"allow_substrings": ["*referencia"]},
    "dim_digito_identificador": {"allow_substrings": ["*di"]},
    "dim_equipamentos": {"allow_substrings": ["*equipamento*"]},
    "dim_empresas": {
        "allow_substrings": ["*empresa*"],
        "deny_substrings": ["*origem*", "*destino*"],
    },
    "dim_aerodromos_origem": {
        "allow_substrings": ["*aerodromo*", "*origem*"],
        "deny_substrings": ["*destino*", "*escala*"],
    },
    "dim_aerodromos_destino": {
        "allow_substrings": ["*aerodromo*", "*destino*"],
        "deny_substrings": ["*origem*", "*escala*"],
    },
    "dim_aerodromos": {
        "allow_substrings": ["*aerodromo*", "*destino*"],
        "deny_substrings": ["*origem*", "*escala*"],
    },
    "fact_aircraft_moviments": {
        "allow_substrings": ["*"],
        "deny_substrings": [
            "*empresa*",
            "*aerodromo*",
            "*origem*",
            "*destino*",
            "*equipamento*",
            "*di",
            "*referencia",
            "*chegada_real",
            "*partida_real",
        ],
    },
}


_DTYPE_DATATYPE = {
    "string": StringType,
    "float": FloatType,
    "int": IntegerType,
    "date": DateType,
}


def get_column_names(table_name="", normalize=False):
    allow_substrings = _COLUMN_PATTERNS[table_name].get("allow_substrings", [])
    deny_substrings = _COLUMN_PATTERNS[table_name].get("deny_substrings", [])
    columns_to_select = []

    for column_name in _COLUMN_LIST:
        for substring in allow_substrings:
            if fnmatch.fnmatch(column_name, substring) and substring != "":
                logging.info(f"{column_name} - {substring} ALLOW!")
                columns_to_select.append(column_name)
                break

        if column_name in columns_to_select:
            for substring in deny_substrings:
                if fnmatch.fnmatch(column_name, substring) and substring != "":
                    logging.info(f"{column_name} - {substring} DENY!")
                    columns_to_select.remove(column_name)
                    break
    if normalize:
        normalized_columns = []
        for column_name in columns_to_select:
            new_name = remove_substring(column_name, allow_substrings)
            normalized_columns.append(new_name)
        columns_to_select = normalized_columns

    # columns_to_select = sorted(columns_to_select)
    return columns_to_select


def get_raw_schema():
    field_list = [
        StructField(column_name, _DTYPE_DATATYPE["string"](), True)
        for column_name in _COLUMN_LIST
    ]

    return StructType(field_list)


def get_schema(table_name="", normalize=False):
    field_list = []
    column_names = get_column_names(table_name)
    if not fnmatch.filter(column_names, "id_*"):
        column_names.append("id")

    for column in column_names:
        dtype = get_column_dtype(column)
        column = expand_column_name(column)
        column = remove_substring(column, get_allow_substrings(table_name))
        field_list.append(StructField(column, _DTYPE_DATATYPE[dtype](), True))

    return StructType(field_list)


def get_allow_substrings(table_name):
    return _COLUMN_PATTERNS[table_name].get("allow_substrings", [])
