# GRAFMOVE: Graph-based Mobility Optimization and Visualization Engine

To connect our dashboard to your graph:
1. Launch NeoDash
2. Click the 'Load' button (top-left menu)
3. Choose the dashboard.json file of this folder
4. Ensure your Neo4j database is running (default: bolt://localhost:7687)


If you are using Docker and you have a dump of your Neo4j database:



```
docker run --interactive --tty --rm \
    --volume=/data:/data \
    --volume=/backups:/backups \
    --volume=/graph_dumps:/graph_dumps \
    neo4j/neo4j-admin:4.4.4 \
    neo4j-admin load --from=/graph_dumps/*filename.dump* --database=neo4j --force
```


```
docker run \
	--name neo4j_modena \
	--restart always \
	--publish=7474:7474 --publish=7687:7687 \
	--env NEO4J_AUTH=neo4j/*password* \
	--env NEO4J_dbms_security_procedures_unrestricted=jwt.security.*,apoc.*,gds.*,spatial.* \
	-v /data:/data \
	-v /logs:/logs \
	-v /plugins:/plugins \
	--env NEO4JLABS_PLUGINS='["apoc", "graph-data-science"]' \
	--env NEO4J_apoc_import_file_enabled=true \
    neo4j:4.4.4
```

```
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
```