SHELL:=/bin/bash

.PHONY: help
help: ##@miscellaneous Show this help message
	@perl ./help.pl $(MAKEFILE_LIST)

.PHONY: init
init: ##@miscellaneous Create a venv
	pip3 install virtualenv --quiet --no-cache-dir;
	python3 -m venv ./venv;
