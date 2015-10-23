.PHONY: clean dist zip local deps

default: clean dist zip;

deps:
	pip install trollius boto3 -t lib/ --upgrade

dist:
	test -d dist || mkdir dist
	cp *.py dist/
	cp -r lib/* dist/

zip: dist
	cd dist && zip -q -r dist.zip .

clean:
	rm -rf dist.zip dist/*

local:
	python-lambda-local -l lib/ -t 60 main.py event/test.json
