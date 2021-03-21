FROM apache/superset

USER root

COPY superset_requirements.txt .

RUN pip install \
    --quiet \
    --no-cache-dir \
    --requirement superset_requirements.txt

USER superset
