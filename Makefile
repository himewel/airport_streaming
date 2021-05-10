SHELL:=/bin/bash

.PHONY: help
help: ##@miscellaneous Show this help message
	@perl ./help.pl $(MAKEFILE_LIST)

.PHONY: init
init: ##@miscellaneous Create a venv
	pip3 install virtualenv --quiet --no-cache-dir;
	python3 -m venv ./venv;

.PHONY: network
network: ##@docker Create a network for Apache Airflow and Spark cluster
	docker network create airport_streaming
