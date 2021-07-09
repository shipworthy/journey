.PHONY: \
	build \
	dialyzer \
	hex-pm-publish \
	hex-pm-publish-doc \
	hex-pm-revert \
	install-dependencies \
	test \
	watch \
	watch-docks

install-dependencies:
	mix deps.get

build:
	mix compile
	mix docs

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
	mix hex.publish --organization shipworthy

hex-pm-publish-doc:
	mix hex.publish docs --organization shipworthy

hex-pm-revert:
	mix hex.publish --revert $(PUBLISHED_VERSION)
