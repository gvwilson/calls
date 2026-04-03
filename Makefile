.PHONY: docs
all: commands

## scenarios: create all scenarios
scenarios:
	@make plain
	@make followup
	@make newclients
	@make special

## plain: create with no shocks to the system
plain:
	@python sim.py

## followup: create with increase in followup time
followup:
	@python sim.py --shock followup

## newclients: create with new clients
newclients:
	@python sim.py --shock newclients

## special: create with special offer
special:
	@python sim.py --shock special

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
	@rm -f *.db *.html

## fix: fix code issues
fix:
	ruff check --fix .

## format: format code
format:
	ruff format .
