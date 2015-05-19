install:
	npm install

test:
	./node_modules/.bin/mocha --check-leaks --recursive -R list

.PHONY: install test
