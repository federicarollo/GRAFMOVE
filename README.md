# GRAFMOVE: Graph-based Mobility Optimization and Visualization Engine

Create a local DBMS in Neo4j (v4.4.4) and install the plugins APOC (v4.4.0.3) and GDS (v2.0.0).

Add this line in the settings to enable file import:
*apoc.import.file.enabled=true*

Add [neo4j-spatial-0.28.1-neo4j-4.4.3-server-plugin.jar](https://github.com/neo4j-contrib/spatial/releases/download/0.28.1-neo4j-4.4.3/neo4j-spatial-0.28.1-neo4j-4.4.3-server-plugin.jar) in the plugin folder
and add this to settings:
dbms.security.procedures.unrestricted=jwt.security.\*,apoc.\*,gds.\*,**spatial.\***


## Graph creation and integration of additional data

To create the graph:

`python graph/create_footpath_graph.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j --neo4jpwd neo4jpwd --latitude lat --longitude lon --distance dist`

where *lat* and *lot* are the latitude and longitude of the center point and *dist* is the distance from the center point to define the bounding box.

Example to create the graph of Modena:

`python graph/create_footpath_graph.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j --neo4jpwd neo4jpwd --latitude 44.645885 --longitude 10.9255707 --distance 5000`


To add POIs (restaurants, shops, squares and tourist attractions) in the graph:

`python graph/add_point_of_interest.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j --neo4jpwd neo4jpwd --latitude lat --longitude lon --distance dist`


To integrate green areas in the graph:

`python graph/integrate_green_area.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j --neo4jpwd neo4jpwd --latitude lat --longitude lon --distance dist`


## Routing

To find the best path between a set of points:

`python routing/routing.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j --neo4jpwd neo4jpwd --weight weight --points points`

where *weight* is the routing cost function to optimize (e.g., *distance* or *green_area_weight*) and *points* is a list of space-separated OpenStreetMap identifiers of the FootNode nodes of the graph.

The script will return:
- a matrix with the cost between each pair of points,
- a csv file with the path between each pair as sequence of FootNode nodes.

If no value is specified for *points*, all the POIs in the graph are considered and the nearest FootNode node is identified.
If *points='bbox'* then you need to specify the parameters *latitude_min*, *latitude_max*, *longitude_min*, *longitude_max*, to define a bounding box and only the POIs in the bounding box will be extracted.
In these cases, the script will return also the file with the list of POIs and the nearest FootNode node.

Example to find the best paths between all the POIs in the city center of Modena:

`python routing/routing.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j --neo4jpwd neo4jpwd --points bbox --latitude_min 44.640049 --latitude_max 44.652324 --longitude_min 10.917066 --longitude_max 10.934938 --weight distance`


To solve the Traveling Salesperson Problem (TSP), i.e., to identify the best path to visit a set of points once and only once:

`python routing/tsp.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd neo4jpwd --points points`

where *points* is the sequence of space-separated OpenStreetMap identifiers of the FootNode nodes of the points to visit.

If no value is specified for *points*, N points are selected randomly from the FootNode nodes near the POIs. The default number of points is 5, but this number can be modified through the *num_points* parameter.

Note that the execution time of this script increases exponentially as the number of points grows. The script takes less than 40 seconds for up to 9 points, but from 10 points onward, the time increases significantly (8 minutes for 10 points), as does the memory usage.

The script will create the map with the optimal path and will store the path as sequence of FootNode nodes in a csv file.



**Note**: If you are creating a graph for a relatively large area, consider using a local instance of the Overpass API, which you can be installed on your device via Docker (https://github.com/wiktorn/Overpass-API).
Consequently, you will need to modify the following line of code *api = overpy.Overpass()* in the files *integrate_green_area.py* and *add_point_of_interest.py*, assigning the URL of your local Overpass API instance (e.g., http://localhost:12346/api/interpreter) to the URL parameter within the parentheses.
