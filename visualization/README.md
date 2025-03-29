# GRAFMOVE: Graph-based Mobility Optimization and Visualization Engine

To connect our dashboard to your graph:
1. Launch NeoDash
2. Click the 'Load' button (top-left menu)
3. Choose the dashboard.json file of this folder
4. Ensure your Neo4j database is running (default: bolt://localhost:7687)


If you are using Docker and you have a dump of your Neo4j database:



`
docker run --interactive --tty --rm \
    --volume=/data:/data \
    --volume=/backups:/backups \
    --volume=/graph_dumps:/graph_dumps \
    neo4j/neo4j-admin:4.4.4 \
    neo4j-admin load --from=/graph_dumps/*cityname.dump* --database=neo4j --force
`


`
docker run --interactive --tty --rm \
	--name *neo4j_4_4_4_cityname* \
    --publish=7474:7474 --publish=7687:7687 \
    --env NEO4J_AUTH=neo4j/*password* \
	--env NEO4J_dbms_routing_enabled=true \
	--env NEO4J_dbms_routing_default_router=BOLT \
	--env NEO4J_dbms_routing_advertised_address=localhost:7687 \
    --volume=/logs/:/logs \
    --volume=/import/:/import \
	--volume=/data:/data \
    neo4j:4.4.4
`

`
docker run  -it --rm -p 5005:5005 \
    -e ssoEnabled=false \
    -e ssoProviders=[] \
    -e ssoDiscoveryUrl="https://example.com" \
    -e standalone=true \
    -e standaloneProtocol="neo4j" \
    -e standaloneHost="localhost" \
    -e standalonePort="7687" \
    -e standaloneDatabase="neo4j" \
    -e standaloneDashboardName="GRAFMOVE" \
    -e standaloneDashboardDatabase="neo4j" \
    -e standaloneAllowLoad=false \
    -e standaloneLoadFromOtherDatabases=false \
    -e standaloneMultiDatabase=false \
    neo4jlabs/neodash
`