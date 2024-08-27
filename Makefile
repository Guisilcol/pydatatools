
.PHONY: venv unit-test integration-test


default:
	@echo "make venv"
	@echo "	create virtual environment"
	@echo "make unit-test"
	@echo "	run unit tests"
	@echo "make integration-test"
	@echo "	run integration tests"

venv:
	python3 -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt

unit-test:
	python -m pytest -vv tests/unit

