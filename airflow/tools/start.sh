#!/usr/bin/env bash

echo "Setting up meta database..."
airflow db upgrade

echo "Creating default user..."
airflow users create \
    --role Admin \
    --username admin \
    --password admin \
    --firstname First \
    --lastname Name \
    --email name@domain.com

echo "Clean old .err and .pid files..."
rm $AIRFLOW_HOME/*.pid $AIRFLOW_HOME/*.err

echo "Starting services..."
airflow webserver -D & airflow scheduler
