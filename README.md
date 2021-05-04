# Extraction, streaming and visualization of ANAC open database

<code><img src="./img/docker.png"> Docker</code> +
<code><img src="./img/airflow.png"> Airflow</code> +
<code><img src="./img/pyspark.png"> PySpark</code> +
<code><img src="./img/bigquery.png"> BigQuery</code> +
<code><img src="./img/superset.png"> Superset</code>

ANAC (*Agência Nacional de Aviação Civil*, brazilian National Civil Aviation Agency) presents monthly data about civil flights since 2000 to now. In this project, this data is extracted and stored in a staging folder in GCS to be processed in streaming and finally visualized with a dashboard. To do it, Apache Airflow its used to make the monthly extractions and trigger Spark jobs to do the transformantions in the data. To visualize the data, we mirror the processed data also stored in GCS to BigQuery and present a dashboard with Apache Superset. Also, the GCS and BigQuery environments are provided with Terraform while the restant of techs runs locally with docker containers.

About the data, the files distributed by ANAC are *zip* files with *txt* in *csv* format. In the extractions, only the year and month values the base url are changed based on monthly DAG runs. In example, to download the data from january of 2000, we have to `curl` the base url with suffix `basica2000-01.zip`. Next, the zip file is uncompressed and the *txt* is uploaded to GCS in `raw_data` folder. This procedure is realized by the **ExtractionDAG**.

The **StreamingDAG** runs the Spark jobs (with PySpark) first creating a `dim_datas` table with static data about time and then starts the streaming jobs. Each streaming job creates a table grouping columns with related data that will be mirror in BigQuery. At the end, the tables in BigQuery are:

- dim_aerodromos
- dim_digito_identificador
- dim_empresas
- dim_equipamentos
- dim_datas
- fact_voos

## How to use this repo

First, create a GCP service account and place in `credentials/gcloud_credentials.json` (or edit the `.env` to do it). Next, install the requirements and run:

```shell
dotenv -f .env run terraform apply terraform
```

It will setup the the GCS bucket and make able the run of Airflow. So, get up the Spark and Airflow containers and start both ExtractionDAG and StreamingDAG. When the DAGs start to run, they will fill GCS files and BigQuery tables. You can check the Airflow UI in http://localhost:8080 and Spark UI in http://localhost:8888.

```shell
docker-compose up -d airflow spark-worker
```

Once the DAGs have filled the tables in BigQuery, you can check the dashboard in Superset. To do it, you need to build the Superset container. To check Superset UI you can access http://localhost:8000:

```shell
docker-compose up -d superset
```

Finally, to import the presented dashboard you can enter in superset container and run:

```shell
superset import-dashboards -p /opt/superset/dashboard.json
superset import-datasources -p /opt/superset/fact_voos_geo_location.yaml
```

## DAGs

<table>
<tr><td>

`@monthly`
</td><td>

`@once`
</td></tr>
<tr><td>

![image](./img/ExtractionDAG.png)
</td><td>

![image](./img/StreamingDAG.png)
</td></tr></table>

## Dashboard

![image](./img/dashboard.jpg)

## References

- Micro data base url: https://www.gov.br/anac/pt-br/assuntos/regulados/empresas-aereas/envio-de-informacoes/microdados
- Column descriptions: https://www.anac.gov.br/assuntos/setor-regulado/empresas/envio-de-informacoes/descricao-de-variaveis
