SHELL:=/bin/bash

USER_ID=$(shell id -u)
GROUP_ID=$(shell id -g)

export GROUP_ID
export USER_ID
export BQ_DATASET
export BQ_VIEWS
export GOOGLE_CLOUD_PROJECT
export GCS_BUCKET

.PHONY: help
help: ##@miscellaneous Show this help message
	@perl ../help.pl $(MAKEFILE_LIST)

.PHONY: build
build: ##@docker Build docker images
	docker-compose build

.PHONY: up
up: ##@docker Start docker-compose containers
	docker-compose up --detach

.PHONY: gcloud
gcloud: ##@gcloud Calls gcloud auth login
	docker-compose exec \
		airflow \
		gcloud auth login --no-launch-browser --update-adc

.PHONY: stop
stop: ##docker Stop the containers
	docker-compose stop
