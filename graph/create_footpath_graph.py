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
            # result = session.run("""
            #                     MATCH (n:FootNode) SET n.location = point({latitude: tofloat(n.y), longitude: tofloat(n.x)}),
            #                     n.lat = tofloat(n.y), 
            #                     n.lon = tofloat(n.x),
            #                     n.geometry='POINT(' + tofloat(n.y) + ' ' + tofloat(n.x) +')';
            #                     """)

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
            # result = session.run("""
            #                        MATCH (n1:FootNode)-[r:ROUTE]-(n2:FootNode) SET r.distance=point.distance(n1.location, n2.location)
            #                     """)
            
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

    # def set_edge_geometry(conn, graph):
    #     with conn.driver.session() as session:
    #         result = session.run("""MATCH (n1:FootNode)-[r:ROUTE]-(n2:FootNode) SET r.geometry = 'LINESTRING()' """)
    #     return result.values()
    
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
                                    where n.location is not null
                                    CALL spatial.addNode('spatial_node', n) 
                                    YIELD node 
                                    RETURN count(node)
                                """)
            return result.values()

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
    
    graph.import_nodes_in_spatial_layer(neo4jconn)
    print("Imported nodes in spatial layer")
    
    neo4jconn.close_connection()

    return 0


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))
