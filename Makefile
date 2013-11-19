
all: npm-install

npm-install: npm-install-stamp

npm-install-stamp: ./binding.gyp ./src/*
	npm install
	touch npm-install-stamp

test: npm-install
	node_modules/.bin/nodeunit --reporter=minimal test/

clean:
	rm -rf ./build
	rm -f ./zongji.node
	rm -f npm-install-stamp

.PHONY: all npm-install test clean
