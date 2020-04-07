#!/usr/bin/env bash
.PHONY: environment
environment:
	@pyenv install -s 3.7.2
	@pyenv virtualenv 3.7.2 notify-me-poc
	@pyenv local notify-me-poc

.PHONY: install
install:
	@pip install -U -r requirements.txt
