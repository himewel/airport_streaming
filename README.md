# Extraction, streaming and visualization of ANAC open database

ANAC (*Agência Nacional de Aviação Civil*, brazilian National Civil Aviation Agency) presents monthly data about civil flights since 2000 to now. In this project, this data is extracted and stored in a staging folder in GCS to be processed in streaming and finally visualized with a dashboard. To do it, Apache Airflow its used to make the monthly extractions and trigger Spark jobs to do the transformantions in the data. To visualize the data, we mirror the processed data also stored in GCS to BigQuery and present a dashboard with Apache Superset. Also, the GCS and BigQuery environments are provided with Terraform while the restant of techs runs locally with docker containers.

About the data, the files distributed by ANAC are *zip* files with *txt* in *csv* format. In the extractions, only the year and month values the base url are changed based on monthly DAG runs. In example, to download the data from january of 2000, we have to `curl` the base url with suffix `basica2000-01.zip`. Next, the zip file is uncompressed and the *txt* is uploaded to GCS in `raw_data` folder. This procedure is realized by the **ExtractionDAG**.

The **StreamingDAG** runs the Spark jobs (with PySpark) first creating a `dim_datas` table with static data about time and then starts the streaming jobs. Each streaming job creates a table grouping columns with related data that will be mirror in BigQuery. At the end, the tables in BigQuery are:

- dim_aerodromos
- dim_digito_identificador
- dim_empresas
- dim_equipamentos
- dim_datas
- fact_voos

## Dashboard

![image](superset/anac-2021-03-21T23-32-23.824Z.jpg)

## References

- Micro data base url: https://www.gov.br/anac/pt-br/assuntos/regulados/empresas-aereas/envio-de-informacoes/microdados
- Column descriptions: https://www.anac.gov.br/assuntos/setor-regulado/empresas/envio-de-informacoes/descricao-de-variaveis
