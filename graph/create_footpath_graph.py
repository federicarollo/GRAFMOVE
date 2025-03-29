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
                                  "SET n:FootNode", 
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
                                  n.geometry='POINT(' + tofloat(n.y) + ' ' + tofloat(n.x) +')'", 
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
                                    "MATCH (n1:FootNode)-[r:ROUTE]-(n2:FootNode) RETURN r, n1, n2",
                                    "SET r.distance=point.distance(n1.location, n2.location)", 
                                    {batchSize:1000, iterateList:true}
                                    )
                                    YIELD batches, total
                                    RETURN batches, total;
                                """)
            return result.values()

    def set_edge_geometry(conn, graph):
        with conn.driver.session() as session:
            result = session.run("""CALL apoc.periodic.iterate(
                                    "MATCH (a:FootNode)-[r:ROUTE]->(b:FootNode) where a.lat is not null and b.lat is not null return a.lat as lat_a, a.lon as lon_a, b.lat as lat_b, b.lon as lon_b, r",
                                    "set r.geometry='LINESTRING(' + lon_a + ' ' + lat_a + ', ' + lon_b + ' ' + lat_b + ')'", 
                                    {batchSize:1000, iterateList:true}
                                    )
                                    YIELD batches, total
                                    RETURN batches, total;""")
        return result.values()
    
    def set_index(self, conn):
        with conn.driver.session() as session:
            result = session.run("""
                                    CREATE INDEX footnode_id_index FOR (n:FootNode) ON (n.id)
                                """)
            result = session.run("""
                                    CREATE POINT INDEX footnode_location_index FOR (n:FootNode) ON (n.location)
                                """)
            return result.values()
     
    def import_nodes_in_spatial_layer(self, conn):
        with conn.driver.session() as session:
            result = session.run("""
                                MATCH (n:FootNode)
                                where n.is_pedestrian_grafmove='yes'
                                return min(id(n))
                                """)
            min_id = result.values()[0]
            result = session.run("""
                                MATCH (n:FootNode)
                                where n.is_pedestrian_grafmove='yes'
                                return max(id(n))
                                """)
            max_id = result.values()[0]

            limit_max=min_id+1000

            while min_id <= max_id:
                
                result = session.run("""
                                        match (n:FootNode)
                                        where n.location is not null
                                        and n.is_pedestrian_grafmove='yes'
                                        and id(n)>=%s and id(n)<%s
                                        with collect(n) as footnodes
                                        CALL spatial.addNodes('spatial_footnode', footnodes) 
                                        YIELD count 
                                        RETURN count"""%(min_id, limit_max))
                min_id+=1000
                limit_max+=1000

            return result.values()

    def find_connected_components(self, conn):
        with conn.driver.session() as session:
                result = session.run("""
                                    call gds.graph.project.cypher(
                                    'filtered_graph',
                                    'match (n:FootNode) return id(n) as id',
                                    'match (m)-[r:ROUTE]-(n) where 
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
                                    MATCH (n:FootNode)
                                    WHERE n.componentId IS NOT NULL
                                    RETURN n.componentId AS componentId, count(n) AS nodeCount
                                    ORDER BY nodeCount DESC
                                    limit 5""")
                conn_comps = result.values()
                
                return conn_comp_num, conn_comps

    def set_is_pedestrian(self, conn, compId):
        with conn.driver.session() as session:
                result = session.run("""
                                    match (n:FootNode)
                                    with n, case when n.componentId=%s then 'yes' else 'no' end as value
                                    set n.is_pedestrian_grafmove=value"""%(compId))
        


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
    
    neo4jconn.generate_spatial_layer('spatial_footnode')
    
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
    
    graph.set_is_pedestrian(neo4jconn, first_componentId)
    print("Set is_pedestrian_grafmove set")

    graph.import_nodes_in_spatial_layer(neo4jconn)
    print("Imported nodes in spatial layer")
    
    graph.set_edge_geometry(neo4jconn)
    print("Set edge geometry")
    
    neo4jconn.close_connection()

    return 0


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))
