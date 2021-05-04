SHELL:=/bin/bash

export GOOGLE_CLOUD_PROJECT
export MAPBOX_API_KEY

.PHONY: build
build:
	docker-compose build

.PHONY: start
start:
	docker-compose up --detach

.PHONY: shell
shell:
	docker-compose exec superset /bin/bash

.PHONY: gcloud
gcloud:
	docker-compose exec superset \
		gcloud auth login --update-adc --no-launch-browser

.PHONY: stop
stop:
	docker-compose stop
