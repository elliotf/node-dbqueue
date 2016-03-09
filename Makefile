install:
	npm install

clean:
	rm -rf node_modules

lint:
	./node_modules/.bin/jshint .

test:
	npm test

watch:
	./node_modules/.bin/nodemon -x bash -c 'printf "\n\n\n\n\n\n"; make test; date; echo -n ""'

.PHONY: install clean lint test watch
