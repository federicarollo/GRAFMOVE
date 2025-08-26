import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
import osmnx as ox
import argparse
from neo4j import GraphDatabase
from utils.db_utils import Neo4jConnection

class FootPathGraph:

    def create_graph(self, conn, file):
        with conn.driver.session() as session:
            result = session.run("""
                            CALL apoc.import.graphml($file, {storeNodeIds: true, defaultRelationshipType: 'ROUTE', readLabels: true, batchSize: 10000});
                        """, file=file)
            return result.values()

    def set_label(self, conn):
        with conn.driver.session() as session:
            result = session.run("""
                                CALL apoc.periodic.iterate(
                                  "MATCH (n) RETURN n",
                                  "SET n:RouteNode", 
                                  {batchSize:1000, iterateList:true}
                                )
                                YIELD batches, total
                                RETURN batches, total;
                                """)
            return result.values()

    

    def set_location(self, conn):
        with conn.driver.session() as session:
            result = session.run("""
                                CALL apoc.periodic.iterate(
                                  "MATCH (n) RETURN n",
                                  "SET n.location = point({latitude: tofloat(n.y), longitude: tofloat(n.x), srid:4326}), 
                                  n.lat = tofloat(n.y), 
                                  n.lon = tofloat(n.x), 
                                  n.latitude = tofloat(n.y), 
                                  n.longitude = tofloat(n.x), 
                                  n.geometry='POINT(' + tofloat(n.x) + ' ' + tofloat(n.y) +')'", 
                                  {batchSize:1000, iterateList:true}
                                )
                                YIELD batches, total
                                RETURN batches, total;
                                """)
            return result.values()



    def set_distance(self, conn):
        """insert the distance in the nodes' relationships."""
        with conn.driver.session() as session:            
            result = session.run("""
                                    CALL apoc.periodic.iterate(
                                    "MATCH (n1:RouteNode)-[r:ROUTE]-(n2:RouteNode) RETURN r, n1, n2",
                                    "SET r.distance=point.distance(n1.location, n2.location)", 
                                    {batchSize:1000, iterateList:true}
                                    )
                                    YIELD batches, total
                                    RETURN batches, total;
                                """)
            return result.values()

    def set_edge_geometry(self, conn):
        with conn.driver.session() as session:
            result = session.run("""CALL apoc.periodic.iterate(
                                    "MATCH (a:RouteNode)-[r:ROUTE]->(b:RouteNode) where a.lat is not null and b.lat is not null 
                                    return a.lat as lat_a, a.lon as lon_a, b.lat as lat_b, b.lon as lon_b, r",
                                    "set r.geometry='LINESTRING(' + lon_a + ' ' + lat_a + ', ' + lon_b + ' ' + lat_b + ')'", 
                                    {batchSize:1000, iterateList:true}
                                    )
                                    YIELD batches, total
                                    RETURN batches, total;""")
            return result.values()
    
    def set_index(self, conn):
        with conn.driver.session() as session:
            result = session.run("""
                                    CREATE INDEX osmnode_id_index FOR (n:RouteNode) ON (n.id)
                                """)
            result = session.run("""
                                    CREATE POINT INDEX osmnode_location_index FOR (n:RouteNode) ON (n.location)
                                """)
            return result.values()
     
    def import_nodes_in_spatial_layer(self, conn):
        with conn.driver.session() as session:
            result = session.run("""
                                MATCH (n:RouteNode)
                                where n.pedestrian_allowed_grafmove='yes' or n.cyclist_allowed_grafmove='yes'
                                return toInteger(min(id(n)))
                                """)
            min_id = result.values()[0][0]
            
            result = session.run("""
                                MATCH (n:RouteNode)
                                where n.pedestrian_allowed_grafmove='yes' or n.cyclist_allowed_grafmove='yes'
                                return toInteger(max(id(n)))
                                """)
            max_id = result.values()[0][0]

            limit_max=min_id+1000

            while min_id <= max_id:
                
                result = session.run("""
                                        match (n:RouteNode)
                                        where n.location is not null
                                        and (n.pedestrian_allowed_grafmove='yes' or n.cyclist_allowed_grafmove='yes')
                                        and id(n)>=%s and id(n)<%s
                                        with collect(n) as nodes
                                        CALL spatial.addNodes('spatial_footbikenode', nodes) 
                                        YIELD count 
                                        RETURN count"""%(min_id, limit_max))
                min_id+=1000
                limit_max+=1000

            print(result.values())
            

    def find_connected_components(self, conn):
        with conn.driver.session() as session:
            
            conn.drop_projection("filtered_graph")
            
            result = session.run("""
                                call gds.graph.project.cypher(
                                'filtered_graph',
                                'match (n:RouteNode) return id(n) as id',
                                'match (m)-[r:ROUTE]->(n) where 
                                not r.highway in ["motorway", "motorway_link", "motorway_junction", 
                                "trunk", "trunk_link", "primary", "primary_link", "secondary", "secondary_link", 
                                "busway", "bus_guideway", "bus_stop", 
                                "escape", "raceway", "corridor", "services", "emergency_bay", "proposed", "construction"]
                                return id(n) as source, type(r) as type, id(m) as target') """)
                                
            result = session.run("""
                                CALL gds.wcc.write('filtered_graph', { writeProperty: 'componentId' })
                                YIELD nodePropertiesWritten, componentCount;""")
            print(result)
            conn_comp_num = result.values()[0][1]
            
            result = session.run("""
                                MATCH (n:RouteNode)
                                WHERE n.componentId IS NOT NULL
                                RETURN n.componentId AS componentId, count(n) AS nodeCount
                                ORDER BY nodeCount DESC
                                limit 5""")
            conn_comps = result.values()
            
            return conn_comp_num, conn_comps

    def set_foot_and_bike(self, conn, compId):
        with conn.driver.session() as session:
            
            result = session.run("""CALL apoc.periodic.iterate(
                                "match (n:RouteNode) return n, case when n.componentId=%s then 'yes' else 'no' end as value",
                                "set n.pedestrian_allowed_grafmove=value, n.cyclist_allowed_grafmove=value", 
                                {batchSize:1000, iterateList:true}
                                )
                                YIELD batches, total
                                RETURN batches, total;"""%(compId))
        
            result = session.run("""CALL apoc.periodic.iterate(
                                "match (n:RouteNode) where n.pedestrian_allowed_grafmove='yes' return n",
                                "set n:FootNode", 
                                {batchSize:1000, iterateList:true}
                                )
                                YIELD batches, total
                                RETURN batches, total;""")
        
            result = session.run("""CALL apoc.periodic.iterate(
                                "match (n:RouteNode) where n.cyclist_allowed_grafmove='yes' return n",
                                "set n:BikeNode", 
                                {batchSize:1000, iterateList:true}
                                )
                                YIELD batches, total
                                RETURN batches, total;""")
                                    
    def classify_roads(self, conn):
        with conn.driver.session() as session:
            
                result = session.run("""CALL apoc.periodic.iterate(
                                    "match (:FootNode)-[r:ROUTE]-(:FootNode) return r, 
                                    case 
                                    when r.highway in ['pedestrian', 'footway', 'steps'] then 1
                                    when r.highway='living_street' and r.foot='yes' and r.segregated='yes' then 1
                                    when r.highway='path' and r.foot='yes' and r.segregated='yes' then 1
                                    when r.highway='track' and r.foot='yes' and r.segregated='yes' then 1
                                    when r.foot='designated' then 1
                                    when r.footway='sidewalk' then 1
                                    when r.sidewalk in ['left', 'right', 'both', 'yes', 'lane', 'separate'] then 1
                                    when r.foot='yes' then 2
                                    when r.highway='footway' and r.bicycle='yes' then 2
                                    when r.bicycle='designated' and r.segregated='no' then 2
                                    when r.highway in ['residential', 'unclassified', 'path', 'track', 'service', 'living_street'] then 2
                                    when r.highway='living_street' and r.foot='yes' and r.segregated='no' then 2
                                    when r.highway='path' and r.foot='yes' and r.segregated='no' then 2
                                    when r.highway='track' and r.foot='yes' and r.segregated='no' then 2
                                    when toInteger(r.maxspeed)<=30 then 2
                                    when toInteger(r.maxspeed)>30 and toInteger(r.maxspeed)<=50 then 3
                                    when toInteger(r.maxspeed)>50 then 4
                                    else 5
                                    end as class",
                                    "set r.foot_class=class", 
                                    {batchSize:1000, iterateList:true}
                                    )
                                    YIELD batches, total
                                    RETURN batches, total;""")
                                    
                                    
                result = session.run("""CALL apoc.periodic.iterate(
                                    "match (:BikeNode)-[r:ROUTE]-(:BikeNode) return r, 
                                    case 
                                    when r.highway='cycleway' then 1
                                    when r.cycleway='track' then 1
                                    when r.cycleway_right='track' then 1
                                    when r.cycleway_left='track' then 1
                                    when r.cycleway_both='track' then 1
                                    when r.bicycle='use_sidepath' then 1
                                    when r.bicycle='designated' and r.segregated='yes' then 1
                                    when r.cycleway='lane' then 2
                                    when r.cycleway_left='lane' then 2
                                    when r.cycleway_right='lane' then 2
                                    when r.cycleway_both='lane' then 2
                                    when r.cycleway='share_busway' then 2
                                    when r.cycleway_left='share_busway' then 2
                                    when r.cycleway_right='share_busway' then 2
                                    when r.highway='footway' and r.bicycle='yes' then 2
                                    when r.highway in ['residential', 'unclassified', 'path', 'track', 'service', 'living_street'] then 2
                                    when r.bicycle='designated' and r.segregated='no' then 2
                                    when toInteger(r.maxspeed)<=30 then 2
                                    when toInteger(r.maxspeed)>30 and toInteger(r.maxspeed)<=50 then 3
                                    when toInteger(r.maxspeed)>50 then 4
                                    else 5
                                    end as class",
                                    "set r.bike_class=class", 
                                    {batchSize:1000, iterateList:true}
                                    )
                                    YIELD batches, total
                                    RETURN batches, total;""")


def add_options():
    parser = argparse.ArgumentParser(description='Creation of the graph.')
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of city center""",
                        required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of city center""",
                        required=True)
    parser.add_argument('--distance', '-d', dest='dist', type=float,
                        help="""Insert distance (in meters) of the area to be cover""",
                        required=True)
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .graphml file to store the graph.""",
                        required=False, default='graph.graphml')
    return parser


def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    
    neo4jconn = Neo4jConnection(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    neo4jconn.open_connection()
    path = neo4jconn.get_path()[0][0] + '/' + neo4jconn.get_import_folder_name()[0][0] + '/' + options.file_name
    print(path)
    
    G = ox.graph_from_point((options.lat, options.lon),
                            dist=int(options.dist),
                            dist_type='bbox',
                            simplify=False,
                            network_type='all',
                            retain_all=True
                            )
    ox.save_graphml(G, path)
    
    neo4jconn.generate_spatial_layer('spatial_footbikenode')
     
    graph = FootPathGraph()
    graph.create_graph(neo4jconn, options.file_name)
    print("Graph created")
    
    graph.set_label(neo4jconn)
    print("Label set")
    
    graph.set_location(neo4jconn)
    print("Location set")
    
    graph.set_distance(neo4jconn)
    print("Distance set")
    
    graph.set_index(neo4jconn)
    print("Index set")
    
    cc_num, ccomponents = graph.find_connected_components(neo4jconn)
    print("Number of connected components: " + str(cc_num))
    print("Top 5 connected components sorted by number of nodes:")
    print("ComponentId\tNumber of nodes")
    for comp in ccomponents:
        print(str(comp[0]) + "\t\t" + str(comp[1]))
    
    first_componentId = ccomponents[0][0]
    
    graph.set_foot_and_bike(neo4jconn, first_componentId)
    print("Set is_pedestrian_grafmove set")
    
    graph.classify_roads(neo4jconn)
    print("Roads classified")

    graph.import_nodes_in_spatial_layer(neo4jconn)
    print("FootNodes imported in spatial layer")
    
    graph.set_edge_geometry(neo4jconn)
    print("Set edge geometry")
    
    neo4jconn.close_connection()

    return 0


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))
