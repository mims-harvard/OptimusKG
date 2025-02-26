#!/bin/bash

# turn on bash's job control
set -m

# Start the primary process and put it in the background
# Reference: 
# (1) https://github.com/neo4j/docker-neo4j-publish/blob/87bc06f60e80315e92d65af32b60af222c83ad52/5.26.2/bullseye/community/Dockerfile#L55
# (2) https://github.com/neo4j/docker-neo4j-publish/blob/87bc06f60e80315e92d65af32b60af222c83ad52/5.26.2/bullseye/community/Dockerfile#L14C24-L14C33
/startup/docker-entrypoint.sh neo4j &

# Wait for Neo4j to be ready
echo "Waiting for Neo4j to be ready..."
until wget --no-verbose --tries=1 --spider http://localhost:7474 2>/dev/null; do
    echo "Neo4j is unavailable - sleeping"
    sleep 10
done

# Import the graphml file
# TODO: Parameterize the graphml file
echo "Neo4j is up - executing import"
until cypher-shell -u neo4j "CALL apoc.import.graphml('/var/lib/neo4j/import/ontology_structure.graphml', {readLabels: true, readRelationshipTypes: true})"
do
    echo "Import failed - retrying"
    sleep 10
done
echo "Import completed"

# Bring the primary process back into the foreground and leave it there
fg %1


