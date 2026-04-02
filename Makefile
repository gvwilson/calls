.PHONY: docs
all: commands

## simple: re-create database with no shocks to the system
noshock:
	@python sim.py --db calls.db

## newclients: re-create database with new clients
newclients:
	@python sim.py --db calls.db --shock newclients

## followup: re-create database with increase in followup time
followup:
	@python sim.py --db calls.db --shock followup

## ---: ---

## commands: show available commands (*)
commands:
	@grep -h -E '^##' ${MAKEFILE_LIST} \
	| sed -e 's/## //g' \
	| column -t -s ':'

## check: check code issues
check:
	@ruff check .

## clean: clean up
clean:
	@rm -rf ./dist ./tmp
	@find . -path './.venv' -prune -o -type d -name '__pycache__' -exec rm -rf {} +
	@find . -path './.venv' -prune -o -type f -name '*~' -exec rm {} +
	@rm -f *.db

## fix: fix code issues
fix:
	ruff check --fix .

## format: format code
format:
	ruff format .
