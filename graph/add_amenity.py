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
            CALL apoc.periodic.iterate(
            "CALL apoc.load.json('amenity_nodes.json') 
            YIELD value
            UNWIND value.elements AS nodo
            return nodo",
            "MERGE (n:OSMNode:POI {osm_id: nodo.id})
            ON CREATE SET n.name=nodo.tags.name,
            n.lat=tofloat(nodo.lat), 
            n.lon=tofloat(nodo.lon), 
            n.geometry= 'POINT(' + nodo.lat + ' ' + nodo.lon +')'
            WITH n, nodo
            MERGE (n)-[:TAGS]->(t:Tag)
            ON CREATE SET t += nodo.tags", 
            {batchSize:100, iterateList:true, parallel:false}
            )
            YIELD batches, total
            RETURN batches, total;
            """

            result = session.run(query)
            return result.values()

    def import_node_way(self, conn):
        """import nodes of the ways in the graph."""
        with conn.driver.session() as session:
            query = """ 
            CALL apoc.periodic.iterate(
            "CALL apoc.load.json('nodes_of_ways.json') 
            YIELD value
            UNWIND value.elements AS node
            return node",
            "MERGE (n:OSMNode {osm_id: node.id})
            ON CREATE SET 
            n.lat = toFloat(node.lat),
            n.lon = toFloat(node.lon),
            n.geometry = 'POINT(' + node.lat + ' ' + node.lon +')'", 
            {batchSize:100, iterateList:true, parallel:false}
            )
            YIELD batches, total
            RETURN batches, total;
            """

            result = session.run(query)
            return result.values()

    def import_way(self, conn):
        """import amenities as ways in the graph."""
        with conn.driver.session() as session:
            query = """
            CALL apoc.periodic.iterate(
            "CALL apoc.load.json('amenity_ways.json') 
            YIELD value
            UNWIND value.elements AS el
            return el",
            "MERGE (w:OSMWay:POI {osm_id: el.id}) 
            ON CREATE SET w.name = el.tags.name
            MERGE (w)-[:TAGS]->(t:Tag) 
            ON CREATE SET t += el.tags
            WITH w, el.nodes AS nodes
            UNWIND nodes AS node
            MATCH (n:OSMNode {osm_id: node})
            MERGE (n)-[:PART_OF]->(w)", 
            {batchSize:100, iterateList:true, parallel:false}
            )
            YIELD batches, total
            RETURN batches, total;
            """

            result = session.run(query)
            return result.values()

    def import_nodes_into_spatial_layer(self, conn):
        """Import OSM nodes nodes in a Neo4j Spatial Layer"""
        with conn.driver.session() as session:
            result = session.run("""
                                MATCH (n:OSMNode)
                                return toInteger(min(id(n)))
                                """)
            min_id = result.values()[0][0]
            result = session.run("""
                                MATCH (n:OSMNode)
                                return toInteger(max(id(n)))
                                """)
            max_id = result.values()[0][0]

            limit_max=min_id+1000

            while min_id <= max_id:
                
                result = session.run("""
                                        match (n:OSMNode)
                                        where n.location is not null
                                        and id(n)>=%s and id(n)<%s
                                        with collect(n) as osmnodes
                                        CALL spatial.addNodes('spatial_osmnode', osmnodes) 
                                        YIELD count 
                                        RETURN count
                                    """%(min_id, limit_max))
                min_id+=1000
                limit_max+=1000

            return result.values()
            

    def connect_amenity(self, conn):
        """Connect the POIs to the nearest RouteNodes in the graph."""
        with conn.driver.session() as session:
            result = session.run("""
                                CALL apoc.periodic.iterate(
                                    "MATCH (p:OSMNode) RETURN p",
                                    "CALL spatial.withinDistance('spatial_footbikenode', p.location, 0.2) YIELD node, distance
                                    with p, collect(node) as nodes, collect(distance) as distances 
                                with p, nodes[0] as nearbyRouteNode, distances[0] as nearDistance
                                     MERGE (p)-[r:NEAR]->(nearbyRouteNode)
                                     ON CREATE SET r.distance = nearDistance", 
                                    {batchSize:500, iterateList:true, parallel:false}
                                )
                                YIELD batches, total
                                RETURN batches, total;
                                """)
            return result.values()

    def set_index(self, conn):
        """create index on nodes"""
        with conn.driver.session() as session:
            result = session.run("""
                                   CREATE POINT INDEX osmnode_location_index FOR (p:OSMNode) ON (p.location);
                                """)
            return result.values()

    def set_location(self, conn):
        """create index on nodes"""
        with conn.driver.session() as session:
            result = session.run("""
                                CALL apoc.periodic.iterate(
                                "MATCH (c:OSMNode) return c",
                                "SET c.location = point({latitude: c.lat, longitude: c.lon, srid:4326}), 
                                c.latitude = tofloat(c.lat),
                                c.longitude = tofloat(c.lon)", 
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
    path = neo4jconn.get_path()[0][0] + '/' + neo4jconn.get_import_folder_name()[0][0] + '/' #+ options.file_name
    
    amenity = Amenity()
    
    dist = options.dist
    lon = options.lon
    lat = options.lat
    
    api = overpy.Overpass()
    result = api.query(f"""(   
                           way(around:{dist},{lat},{lon})["amenity"];
                           way(around:{dist},{lat},{lon})["place"="square"];
                           way(around:{dist},{lat},{lon})["tourism"];
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
    
    print("Number of ways: " + str(len(list_ways)))
    
    
    
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
    list_nodes_json = {"elements": list_nodes}
    
    print("Number of nodes " + str(len(list_nodes)))
    
    
    
    with open(path + 'nodes_of_ways.json', "w") as f:
        json.dump(list_node_way_json, f)
    
    with open(path + "amenity_ways.json", "w") as f:
        json.dump(list_ways_json, f)
    
    with open(path + 'amenity_nodes.json', "w") as f:
        json.dump(list_nodes_json, f)
    
    
    amenity.import_node_way(neo4jconn)
    print("Imported nodes of ways")
    
    amenity.import_way(neo4jconn)
    print("Imported ways")
    
    amenity.import_node(neo4jconn)
    print("Imported nodes")
    
    amenity.set_location(neo4jconn)
    print("Location set")
    
    neo4jconn.generate_spatial_layer('spatial_osmnode')
    print("Generated spatial layer")
    
    res = amenity.import_nodes_into_spatial_layer(neo4jconn)
    print("OSMNodes imported in the spatial layer")
    
    # amenity.set_index(neo4jconn)
    # print("Index set")

    amenity.connect_amenity(neo4jconn)
    print("Amenity connected")

    neo4jconn.close_connection()

    return 0


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))
