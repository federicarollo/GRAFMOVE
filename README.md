# GRAFMOVE: Graph-based Mobility Optimization and Visualization Engine

Create a local DBMS in Neo4j (v4.4.4)
Install APOC and GDS
Modify settings to enable file import:
- add line "apoc.import.file.enabled=true"

add spatial in plugins folder
neo4j-spatial-0.28.1-neo4j-4.4.3-server-plugin.jar
and add this to settings
dbms.security.procedures.unrestricted=jwt.security.*,apoc.*,gds.*,spatial.*




Creating the graph:
python graph/create_footpath_graph.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd neo4jpwd --latitude 44.645885 --longitude 10.9255707 --distance 5000


python graph/add_amenity.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd neo4jpwd --latitude 44.645885 --longitude 10.9255707 --distance 5000



python graph/integrate_green_area.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd neo4jpwd


FootNode anzichè roadjunction
POI anzichè PointOfInterest





Adding amenities to the graph
python amenity.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd footpath_osmnx_also_private --latitude 44.645885 --longitude 10.9255707 --distance 5000


Selecting amenities of interest:
MATCH (f:RoadJunction)<-[r:NEAR]-(:OSMWayNode)<-[:MEMBER]-(n:PointOfInterest)-[:TAGS]-(t:Tag) 
where n.name is not null and 
(t.tourism='attraction' or t.amenity='place_of_worship' or t.place='square' or t.amenity='fountain')
with n.name as poi_name, collect(f.id) as footnodes, collect(r.distance) as distances
with poi_name, footnodes, distances, apoc.coll.indexOf(distances,min(distances)) as min_index 
with poi_name, footnodes[min_index] as footnode, distances[min_index] as dist
match (rj:RoadJunction {id: footnode}), (poi:PointOfInterest {name: poi_name}) 
return collect([rj.lat, rj.lon]), collect(poi.name), collect(rj.id) as osm_id