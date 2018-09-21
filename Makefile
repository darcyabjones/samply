export SAMPLY_DB=postgresql://postgres:example@localhost:5432/postgres

all: locations contributors

init:
	samply init

locations: init
locations: data/locations.csv
	samply -vv add locations $<

contributors: init
contributors: data/contributors.csv
	samply -vv add contributors $<
