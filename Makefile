.PHONY: docs
all: commands

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

## noshock: re-create database with no shocks to the system
noshock:
	@python sim.py --db calls.db

## followup: re-create database with increase in followup time
followup:
	@python sim.py --db calls.db --shock followup

## fix: fix code issues
fix:
	ruff check --fix .

## format: format code
format:
	ruff format .

## test: run tests
test:
	pytest tests
