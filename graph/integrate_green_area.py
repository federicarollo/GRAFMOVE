import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
import osmnx as ox
import argparse
from neo4j import GraphDatabase
from utils.db_utils import Neo4jConnection
from utils.path_utils import PathUtils
import json
import pandas as pd
import requests
import overpy
from tqdm import tqdm



class GreenArea:
        
    def set_weight(self, conn):
        with conn.driver.session() as session:

            query = """CALL apoc.periodic.iterate(
            "MATCH (r1:FootNode)-[r:ROUTE]-(r2:FootNode) return r1, r2, r",
            "set r.green_area = 0", 
            {batchSize:1000, iterateList:true}
            )
            YIELD batches, total
            RETURN batches, total;"""
            session.run(query)

            query = """CALL apoc.periodic.iterate(
            "MATCH (r1:FootNode)-[r:ROUTE]-(r2:FootNode) where r1.green_area = 'yes' or r2.green_area = 'yes' return r1, r2, r",
            "set r.green_area = 50", 
            {batchSize:1000, iterateList:true}
            )
            YIELD batches, total
            RETURN batches, total;"""
            session.run(query)

            query = """CALL apoc.periodic.iterate(
            "MATCH (r1:FootNode)-[r:ROUTE]-(r2:FootNode) where r1.green_area = 'yes' and r2.green_area = 'yes' return r1, r2, r",
            "set r.green_area = 100", 
            {batchSize:1000, iterateList:true}
            )
            YIELD batches, total
            RETURN batches, total;"""
            session.run(query)


            query = """CALL apoc.periodic.iterate(
            "MATCH ()-[r:ROUTE]-() return r",
            "set r.green_area_weight = r.distance / (r.green_area/100 + 1)", 
            {batchSize:1000, iterateList:true}
            )
            YIELD batches, total
            RETURN batches, total;"""
            session.run(query)

            
    def find_matching_footnodes(self, conn, footnodes_list):
        with conn.driver.session() as session:
            result = session.run("""
            WITH $footnodes_list AS footnodes_list
            CALL apoc.periodic.iterate(
              "UNWIND $footnodes_list AS f
               MATCH (n:FootNode)
               WHERE n.id = toString(f)
               RETURN n",
              "SET n.green_area = 'yes'",
              {batchSize: 1000, params: {footnodes_list: footnodes_list}}
            )
            YIELD batches, total
            RETURN batches, total;
             """, footnodes_list=footnodes_list)
            return result.values()[0]


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
    path = neo4jconn.get_path()[0][0] + '/' + neo4jconn.get_import_folder_name()[0][0] + '/'
    
    greenarea = GreenArea()
    
    lat = options.lat
    lon = options.lon
    dist = options.dist
    
    query = f"""
    [out:json];
    (
      nwr(around:{dist},{lat},{lon})[landuse~"grass|flowerbed|meadow|forest|vineyard|village_green|recreation_ground|orchard|nature_reserve"];
      nwr(around:{dist},{lat},{lon})[leisure~"garden|park|dog_park|pitch|nature_reserve|golf_course|garden"];
      nwr(around:{dist},{lat},{lon})[natural~"wood|tree_row|scrub|heath|grassland|fell"];
      nwr(around:{dist},{lat},{lon})[barrier=hedge];
    );
    out geom;
    """
    print(query)
                           
    url = 'http://overpass-api.de/api/interpreter'
    result = requests.get(url, params={'data': query})
    data = result.json()['elements']
    
    nodes_in_green_area = [str(elem['id']) for elem in data if elem['type']=='node']
    print("Number of nodes of green areas: " + str(len(nodes_in_green_area)))
    
    ways_of_green_area = [PathUtils.elem_to_feature(elem, "Polygon") for elem in data if elem['type']=='way'] # or elem['type']=='relation']
    print("Number of ways of green areas: " + str(len(ways_of_green_area)))
    
    
    api = overpy.Overpass()#url="http://localhost:12350/api/interpreter") # if you are using a local istance of overpass, add this with proper port: url="http://localhost:12346/api/interpreter"
    polygons = []
    queries = []
    start_query = """[out:json]; ( """
    end_query = """ ); out geom; """
    
    query = start_query
    for i in tqdm(range(len(ways_of_green_area)), desc='Define polygons of green area'):
        poly = ''
        if (len(ways_of_green_area[i])>2):
            for j in range(len(ways_of_green_area[i])):
                poly += str(ways_of_green_area[i][j][0]) + " " + str(ways_of_green_area[i][j][1]) + ' '
            
            if poly not in polygons:
                polygons.append(poly)
                query += f"""
                node(poly:\"{poly.strip()}\");
                """
                if(len(query)+len(end_query) > 20000):
                    query += end_query
                    queries.append(query)
                    query = start_query
    
    
    if(not query.endswith(end_query)):
        query += end_query
        queries.append(query)
    print("Number of queries to perform: " + str(len(queries)))
    
    for query in queries:
        # success = 0
        # while success == 0:
        #     try:
        #         result = api.query(query)
        #         # result = requests.get(url, params={'data': query})
        #         # data = result.json()['elements']
        #         for node in result.nodes:
        #             nodes_in_green_area.append(str(node.id))
        #         success = 1
        #     except overpy.exception.OverpassGatewayTimeout as e:
        #         time.sleep(5)
        #     except overpy.exception.OverpassTooManyRequests as e:
        #         time.sleep(5)
        result = api.query(query)
        print("Number of nodes retrieved by the query: " + str(len(result.nodes)))
        for node in result.nodes:
            nodes_in_green_area.append(str(node.id))
            
    with open(path + 'nodes_in_green_area.csv', "w") as f:
        json.dump(nodes_in_green_area, f)

    print("Total number of nodes in green area: " + str(len(nodes_in_green_area)))

    count  = greenarea.find_matching_footnodes(neo4jconn, nodes_in_green_area)
    print("Green area property updated to nodes: " + str(count[1]))
    
    
    greenarea.set_weight(neo4jconn)
    
    
    
    neo4jconn.close_connection()
    

    
    
if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))
