SHELL:=/bin/bash

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
	docker-compose exec spark-worker \
		gcloud auth login --update-adc --no-launch-browser

.PHONY: up
stop: ##@docker Stop docker-compose containers
	docker-compose stop
