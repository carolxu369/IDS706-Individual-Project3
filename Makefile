.PHONY: setup test lint

setup:
	pip install -r requirements.txt

test:
	pytest