#!/usr/bin/env bash
rm -rf ./A
rm -rf ./B
unzip R-Neo4j.zip -d ./A
cp -R ./A ./B

#./A/R.Neo4j/bin/neo4j-import --into ./A/R.Neo4j/data/databases/graph.db \
#	--nodes ./csv/group3/publications.csv \
#	--ignore-empty-strings true --multiline-fields true
#
#./B/R.Neo4j/bin/neo4j-import --into ./B/R.Neo4j/data/databases/graph.db \
#	--nodes ./csv/group4/publications.csv \
#	--ignore-empty-strings true --multiline-fields true

./A/R.Neo4j/bin/neo4j-import --into ./A/R.Neo4j/data/databases/graph.db \
	--nodes ./csv/group1/publications.csv \
	--nodes ./csv/group1/researchers.csv \
	--relationships ./csv/group1/relations.csv \
	--ignore-empty-strings true --multiline-fields true

./B/R.Neo4j/bin/neo4j-import --into ./B/R.Neo4j/data/databases/graph.db \
	--nodes ./csv/group2/publications.csv \
	--nodes ./csv/group2/researchers.csv \
	--relationships ./csv/group2/relations.csv \
	--ignore-empty-strings true --multiline-fields true

date
