test.dummy:
	go test -v $$(go list ./... | grep -v /vendor/)

install: ## install project dependency packages
	glide install

test: test.dummy ## run testcases

help:
	@grep -E '^[a-zA-Z0-9._-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
