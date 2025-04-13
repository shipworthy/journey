.PHONY: \
	all \
	build \
	build-test \
	format \
	format-check \
	lint \
	test


POSTGRES_DB_CONTAINER_NAME?=new_journey-postgres-db


all: build format-check lint test


all-clean: clean deps-get all


build:
	mix clean
	mix compile --warnings-as-errors --force
	mix docs --proglang elixir


clean:
	MIX_ENV=test mix clean
	mix clean
	mix deps.clean --all


db-local-psql:
	docker exec -it $(POSTGRES_DB_CONTAINER_NAME) psql -U postgres


db-local-rebuild:
	docker rm -fv $(POSTGRES_DB_CONTAINER_NAME)
	docker run --name $(POSTGRES_DB_CONTAINER_NAME) -p 5438:5432 -e POSTGRES_PASSWORD=postgres -d postgres:16.4


deps-get:
	mix deps.get


format:
	mix format


format-check:
	mix format --check-formatted


lint:
	mix credo --all --strict


test:
	mix test --warnings-as-errors --cover
