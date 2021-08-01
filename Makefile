.PHONY: \
	build \
	dialyzer \
	hex-pm-publish \
	hex-pm-publish-doc \
	hex-pm-publish-doc-private \
	hex-pm-publish-private \
	hex-pm-revert \
	install-dependencies \
	start-postgres \
	test \
	watch \
	watch-docks

install-dependencies:
	mix deps.get

build:
	mix compile
	mix docs

db-setup:
	mix ecto.create
	mix ecto.migrate

test:
	 mix docception README.md
	 mix test --cover

livebook:
	mix escript.install hex livebook
	livebook server

dialyzer:
	 mix dialyzer

watch:
	watch "mix dialyzer && mix docs && mix test --cover"

watch-docs:
	watch mix docs

hex-pm-publish:
	mix hex.publish

hex-pm-publish-doc:
	mix hex.publish docs

hex-pm-publish-private:
	mix hex.publish --organization shipworthy

hex-pm-publish-doc-private:
	mix hex.publish docs --organization shipworthy

hex-pm-revert:
	mix hex.publish --revert $(PUBLISHED_VERSION)

start-postgres:
	docker kill postgres-journey-test || true
	docker run --rm --name postgres-journey-test -e "POSTGRES_PASSWORD=postgres" -p 5432:5432 -d postgres:13.3-buster
