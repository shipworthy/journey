.PHONY: \
	all \
	build \
	build-docs \
	build-test \
	format \
	format-check \
	hex-pm-publish \
	hex-pm-publish-doc \
	hex-pm-revert \
	hex-pm-publish-doc-private \
	hex-pm-publish-private \
	hex-pm-revert-private \
	lint \
	test \
	test-load \
	test-performance \
	validate


POSTGRES_DB_CONTAINER_NAME?=new_journey-postgres-db


all: build format-check lint test test-load


all-clean: clean deps-get all


build:
	mix clean
	mix compile --warnings-as-errors --force
	mix docs --proglang elixir


build-test:
	MIX_ENV=test mix clean
	MIX_ENV=test mix compile --warnings-as-errors --force
	mix docs --proglang elixir


build-docs:
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

dev-team:
	npx @anthropic-ai/claude-code

format:
	mix format


format-check:
	mix format --check-formatted


hex-pm-publish:
	mix hex.publish


hex-pm-publish-doc:
	mix hex.publish docs


hex-pm-revert:
	mix hex.publish --revert $(PUBLISHED_VERSION)


hex-pm-publish-private:
	mix hex.publish --organization shipworthy


hex-pm-publish-doc-private:
	mix hex.publish docs --organization shipworthy


hex-pm-revert-private:
	mix hex.publish --organization shipworthy --revert $(PUBLISHED_VERSION)


lint:
	mix credo --all --strict
	mix hex.outdated || true


test:
	MIX_ENV=test mix clean
	mix test --warnings-as-errors --cover


test-load:
	mix run test_load/sunny_day.exs


test-performance:
	mix run test_load/performance_benchmark.exs

validate: format-check build-test lint test

