import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import overpy
import json
from neo4j import GraphDatabase
import argparse
import time
from utils.db_utils import Neo4jConnection

class Amenity:

    def import_node(self, conn):
        """import POI nodes in the graph."""
        with conn.driver.session() as session:
            query = """
            CALL apoc.load.json("amenity_nodes.json") YIELD value AS value 
            WITH value.elements AS elements
            UNWIND elements AS nodo
            MERGE (n:OSMNode:POI {osm_id: nodo.id})
            ON CREATE SET n.name=nodo.tags.name,
            n.lat=tofloat(nodo.lat), 
            n.lon=tofloat(nodo.lon), 
            n.geometry= 'POINT(' + nodo.lat + ' ' + nodo.lon +')'
            WITH n, nodo
            MERGE (n)-[:TAGS]->(t:Tag)
            ON CREATE SET t += nodo.tags
            """
            result = session.run(query)
            return result.values()

    def import_node_way(self, conn):
        """import nodes of the ways in the graph."""
        with conn.driver.session() as session:
            query = """ 
            CALL apoc.load.json("nodes_of_ways.json") YIELD value AS value 
            WITH value.elements AS elements
            UNWIND elements AS nodo
            MERGE (n:OSMNode {osm_id: nodo.id})
            ON CREATE SET n.lat=tofloat(nodo.lat), 
            n.lon=tofloat(nodo.lon), 
            n.geometry='POINT(' + nodo.lat + ' ' + nodo.lon +')'
            """
            result = session.run(query)
            return result.values()

    def import_way(self, conn):
        """import amenities as ways in the graph."""
        with conn.driver.session() as session:
            query = """
            CALL apoc.load.json("amenity_ways.json") YIELD value 
            WITH value.elements AS elements
            UNWIND elements AS el
            MERGE (w:OSMWay:POI {osm_id: el.id}) 
            ON CREATE SET w.name = el.tags.name
            MERGE (w)-[:TAGS]->(t:Tag) 
            ON CREATE SET t += el.tags
            WITH w, el.nodes AS nodes
            UNWIND nodes AS node
            MATCH (n:OSMNode {osm_id: node})
            MERGE (n)-[:PART_OF]->(w)
            """
            result = session.run(query)
            return result.values()

    def import_nodes_into_spatial_layer(self, conn):
        """Import OSM nodes nodes in a Neo4j Spatial Layer"""
        with conn.driver.session() as session:
            result = session.run("""
                                match(n:OSMNode)
                                CALL spatial.addNode('spatial', n) yield node return node
                                """)
            return result.values()

    def connect_amenity(self, conn):
        """Connect the POIs to the nearest FootNodes in the graph."""
        with conn.driver.session() as session:
            result = session.run("""
                                CALL apoc.periodic.iterate(
                                "MATCH (p:OSMNode) return p",
                                "MATCH (n:FootNode) WHERE point.distance(n.location, p.location) < 100 MERGE (p)-[r:NEAR]->(n) ON CREATE SET r.distance = point.distance(n.location, p.location)", 
                                {batchSize:1000, iterateList:true}
                                )
                                YIELD batches, total
                                RETURN batches, total;
                                """)
            return result.values()

    def set_index(self, conn):
        """create index on nodes"""
        with conn.driver.session() as session:
            # result = session.run("""
            #                        CREATE INDEX FOR (n:OSMWay) ON (n.osm_id)
            #                     """)
            # result = session.run("""
            #                        CREATE INDEX FOR (n:OSMNode) ON (n.osm_id)
            #                     """)
            result = session.run("""
                                   CREATE POINT INDEX osmnode_location_index FOR (p:OSMNode) ON (p.location);
                                """)
            result = session.run("""
                                   CREATE POINT INDEX footnode_location_index FOR (n:FootNode) ON (n.location);
                                """)
            return result.values()

    def set_location(self, conn):
        """create index on nodes"""
        with conn.driver.session() as session:
            result = session.run("""
                                CALL apoc.periodic.iterate(
                                "MATCH (c:OSMNode) return c",
                                "SET c.location = point({latitude: c.lat, longitude: c.lon, srid:4326})", 
                                {batchSize:1000, iterateList:true}
                                )
                                YIELD batches, total
                                RETURN batches, total;
                                """)
            return result.values()


def add_options():
    parser = argparse.ArgumentParser(description='Insertion of POI in the graph.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of city center""",
                        required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of city center""",
                        required=True)
    parser.add_argument('--distance', '-d', dest='dist', type=float,
                        help="""Insert distance (in meters) of the area to be cover""",
                        required=True)
    return parser

def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    neo4jconn = Neo4jConnection(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    neo4jconn.open_connection()
    path = neo4jconn.get_path()[0][0] + '\\' + neo4jconn.get_import_folder_name()[0][0] + '\\' #+ options.file_name
    
    amenity = Amenity()
    
    dist = options.dist
    lon = options.lon
    lat = options.lat
    
    api = overpy.Overpass()
    result = api.query(f"""(   
                           way(around:{dist},{lat},{lon})["amenity"];
                           way(around:{dist},{lat},{lon})["place"="square"];
                           way(around:{dist},{lat},{lon})["tourism"];
                           way(around:{dist},{lat},{lon})["place"="square"];
                           );(._;>;);
                           out body;
						""")
    list_node_way = []
    list_ways = []
    for way in result.ways:
        node_ids = []
        for node in way.get_nodes(resolve_missing=False):
            node_dict = {'id': node.id,
                         'lat': str(node.lat),
                         'lon': str(node.lon)}
            node_ids.append(node.id)
            list_node_way.append(node_dict)
        way_dict = {'id': way.id,
                    'tags': way.tags,
                    'nodes': node_ids}
        list_ways.append(way_dict)
    list_node_way_json = {"elements": list_node_way}
    list_ways_json = {"elements": list_ways}
    
    with open(path + 'nodes_of_ways.json', "w") as f:
        json.dump(list_node_way_json, f)
    amenity.import_node_way(neo4jconn)
    
    with open(path + "amenity_ways.json", "w") as f:
        json.dump(list_ways_json, f)
    amenity.import_way(neo4jconn)
    
    print("Imported " + str(len(list_ways)) + " POIs as ways")
    
    
    
    result = api.query(f"""(   
                           node(around:{dist},{lat},{lon})["amenity"];
                           node(around:{dist},{lat},{lon})["tourism"];
                           node(around:{dist},{lat},{lon})["place"="square"];
                           );
                           out body;
                           """)
    list_nodes = []
    for node in result.nodes:
        d = {'id': node.id,
             'lat': str(node.lat),
             'lon': str(node.lon),
             'tags': node.tags}
        list_nodes.append(d)
    res = {"elements": list_nodes}
    with open(path + 'amenity_nodes.json', "w") as f:
        json.dump(res, f)
    amenity.import_node(neo4jconn)
    
    print("Imported " + str(len(list_nodes)) + " POIs as nodes")
    
    
    amenity.import_nodes_into_spatial_layer(neo4jconn)
    print("Imported nodes in the spatial layer")
    
    amenity.set_location(neo4jconn)
    print("Location set")
    
    amenity.set_index(neo4jconn)

    amenity.connect_amenity(neo4jconn)
    print("Amenity connected")

    neo4jconn.close_connection()

    return 0


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))
