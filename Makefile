SHELL:=/bin/bash

.PHONY: help
help: ##@miscellaneous Show this help message
	@perl ./help.pl $(MAKEFILE_LIST)

.PHONY: init
init: ##@miscellaneous Create a venv
	pip3 install virtualenv --quiet --no-cache-dir;
	python3 -m venv ./venv

.PHONY: network
network: ##@docker Create a network for Apache Airflow and Spark cluster
	docker network create airport_streaming

.PHONY: stream-build
stream-build:
	$(MAKE) -C ./airflow build;
	$(MAKE) -C ./spark build

.PHONY: stream-gcloud
stream-login:
	$(MAKE) -C ./airflow gcloud;
	$(MAKE) -C ./spark gcloud

.PHONY: stream-gcloud
stream-up:
	$(MAKE) -C ./airflow up;
	$(MAKE) -C ./spark up
