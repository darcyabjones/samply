export SAMPLY_DB=postgresql://postgres:example@localhost:5432/postgres

all: samples contributors sample_contribution taxon sample_taxon pesticides sample_pesticides

init:
	samply init

samples: init
samples: data/samples.tsv
	samply -vv add samples $<

contributors: init
contributors: data/contributors.tsv
	samply -vv add contributors $<

sample_contribution: init
sample_contribution: data/sample_contributors.tsv contributors samples
	samply -vv add samplecontribution $<

taxon: init
taxon: data/taxon.tsv
	samply -vv add taxon $<

sample_taxon: init samples taxon
sample_taxon: data/sample_taxon.tsv
	samply -vv add sampletaxon $<

pesticides: init
pesticides: data/pesticides.tsv
	samply -vv add pesticides $<

sample_pesticides: init pesticides samples
sample_pesticides: data/sample_pesticides.tsv
	samply -vv add samplepesticides $<
