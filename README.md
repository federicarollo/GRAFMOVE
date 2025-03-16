# GRAFMOVE: Graph-based Mobility Optimization and Visualization Engine

Create a local DBMS in Neo4j (v4.4.4) and install the plugins APOC (v4.4.0.3) and GDS (v2.0.0).

Add this line in the settings to enable file import: *apoc.import.file.enabled=true*

Add [eo4j-spatial-0.28.1-neo4j-4.4.3-server-plugin.jar](https://github.com/neo4j-contrib/spatial/releases/download/0.28.1-neo4j-4.4.3/neo4j-spatial-0.28.1-neo4j-4.4.3-server-plugin.jar) in the plugin folder
and add this to settings:
dbms.security.procedures.unrestricted=jwt.security.\*,apoc.\*,gds.\*,**spatial.\***




Creating the graph:
python graph/create_footpath_graph.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd modenapass --latitude 44.645885 --longitude 10.9255707 --distance 5000


python graph/add_amenity.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd modenapass --latitude 44.645885 --longitude 10.9255707 --distance 5000



python graph/integrate_green_area.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd modenapass


python routing/routing.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd modenapass --weight distance
python routing/routing.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd modenapass --weight green_area_weight

python routing/routing.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd modenapass --points bbox --latitude_min 44.640049 --latitude_max 44.652324 --longitude_min 10.917066 --longitude_max 10.934938 --weight green_area_weight


points = all
points = bbox (devono esserci lat lon)
points = lista di nodi FootNode --> verificare se funziona

puoi usarlo per calcolare il routing tra coppie di punti che sono footnode
oppure puoi usarlo per il routing tra le amenity (tutte oppure di un bbox)


python routing/tsp.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd modenapass


python utils/visualization.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd modenapass --points "10033394700 10033394686 10033394690 10033394715 4479772518 10033394678 5260701924 5260701926 250857466 6082860448 250846418 9221534513 250857471 6152233507 5088833596 5088833597 5088833598 315731994 315731991 9235768193 10979381512 10658458534 1256903860 10556615834 10556615835 12218692352 250851333 4482988393 4482988394 10543781404"  


neo4jpwd

FootNode anzichè roadjunction
POI anzichè PointOfInterest

MATCH (c:OSMNode) SET c.location = point({latitude: c.lat, longitude: c.lon, srid:4326})



Adding amenities to the graph
python amenity.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd modenapass --latitude 44.645885 --longitude 10.9255707 --distance 5000


Selecting amenities of interest:
MATCH (f:RoadJunction)<-[r:NEAR]-(:OSMWayNode)<-[:MEMBER]-(n:PointOfInterest)-[:TAGS]-(t:Tag) 
where n.name is not null and 
(t.tourism='attraction' or t.amenity='place_of_worship' or t.place='square' or t.amenity='fountain')
with n.name as poi_name, collect(f.id) as footnodes, collect(r.distance) as distances
with poi_name, footnodes, distances, apoc.coll.indexOf(distances,min(distances)) as min_index 
with poi_name, footnodes[min_index] as footnode, distances[min_index] as dist
match (rj:RoadJunction {id: footnode}), (poi:PointOfInterest {name: poi_name}) 
return collect([rj.lat, rj.lon]), collect(poi.name), collect(rj.id) as osm_id




                       #, default = ['10911594251', '10911594425', '10911594443', '10917017001', '10911594239', '10911587741'])
# "6341815000","2021402106","1787128164","5303704141","5304947702","2021511952","315580126","1352341299","1256958142","4712448240","1256958133"
# "10917017350", "10911594286", "10917017015", "10917017269", "10917017335"
# "10681444158", "11065366611", "1627367784", "7619003474", "5905023674", "5477225907", "1344463148", "359743769", "2199223548", "4611974715" --> retrieved on neo4j through amenity



python graph/create_footpath_graph.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd londonpass --latitude 51.5074456 --longitude -0.1277653 --distance 30000
python graph/integrate_green_area.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd londonpass --latitude 51.5074456 --longitude -0.1277653 --distance 30000
