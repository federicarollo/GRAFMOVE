import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
import osmnx as ox
import argparse
from neo4j import GraphDatabase
from utils.db_utils import Neo4jConnection
from utils.utils import Utils
import json
import pandas as pd
import requests
import overpy
from tqdm import tqdm



class GreenArea:
        
    def set_weight(self, conn):
        with conn.driver.session() as session:
            session.run("""
                        MATCH (r1:FootNode)-[r:ROUTE]-(r2:FootNode) set r.green_area = 0 return r
                        """)
            session.run("""
                        MATCH (r1:FootNode)-[r:ROUTE]-(r2:FootNode) where r1.green_area = 'yes' or r2.green_area = 'yes' set r.green_area = 50 return r
                        """)
            session.run("""
                        MATCH (r1:FootNode)-[r:ROUTE]-(r2:FootNode) where r1.green_area = 'yes' and r2.green_area = 'yes' set r.green_area = 100 return r
                        """)
            session.run("""
                        match ()-[r:ROUTE]-() set r.green_area_weight = r.distance / (r.green_area/100 + 1) return count(r)
                        """)
            
    
    def find_matching_footnodes(self, conn, footnodes_list):
        with conn.driver.session() as session:
            result = session.run("""
                                with $footnodes_list as footnodes_list 
                                unwind footnodes_list as f
                                with f 
                                match(n:FootNode) where n.id = toString(f)
                                set n.green_area = \"yes\" 
                                return count(n)
                                """, footnodes_list=footnodes_list)        
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
    return parser


def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    neo4jconn = Neo4jConnection(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    neo4jconn.open_connection()
    path = neo4jconn.get_path()[0][0] + '\\' + neo4jconn.get_import_folder_name()[0][0] + '\\'
    
    greenarea = GreenArea()
    
    query = """
    [out:json];
    area["wikipedia"="it:Modena"]->.area_of_interest;
    (
      nwr(area.area_of_interest)[landuse~"grass|flowerbed|meadow|forest|vineyard|village_green|recreation_ground|orchard|nature_reserve"];
      nwr(area.area_of_interest)[leisure~"garden|park|dog_park|pitch|nature_reserve|golf_course|garden"];
      nwr(area.area_of_interest)[natural~"wood|tree_row|scrub|heath|grassland|fell"];
      nwr(area.area_of_interest)[barrier=hedge];
    );
    out geom;
    """
    
    url = 'http://overpass-api.de/api/interpreter'
    result = requests.get(url, params={'data': query})
    data = result.json()['elements']    
    
    nodes_in_green_area = [str(elem['id']) for elem in data if elem['type']=='node']
    print("Number of nodes of green areas: " + str(len(nodes_in_green_area)))
    
    ways_of_green_area = [Utils.elem_to_feature(elem, "Polygon") for elem in data if elem['type']=='way'] # or elem['type']=='relation']
    print("Number of ways of green areas: " + str(len(ways_of_green_area)))
    
    
    api = overpy.Overpass()
    for i in tqdm(range(len(ways_of_green_area)), desc='ways'):
        poly = ''
        if (len(ways_of_green_area[i])>2):
            for j in range(len(ways_of_green_area[i])):
                poly += str(ways_of_green_area[i][j][0]) + " " + str(ways_of_green_area[i][j][1]) + ' '
            
            query = f"""[out:json];
            (
            node(poly:\"{poly.strip()}\");
            );
            out geom;
            """
            success = 0
            while success == 0:
                try:
                    result = api.query(query)
                    # result = requests.get(url, params={'data': query})
                    # data = result.json()['elements']
                    for node in result.nodes:
                        nodes_in_green_area.append(str(node.id))
                    success = 1
                except overpy.exception.OverpassGatewayTimeout as e:
                    time.sleep(5)
                except overpy.exception.OverpassTooManyRequests as e:
                    time.sleep(5)
    
    
    count  = greenarea.find_matching_footnodes(neo4jconn, nodes_in_green_area)
    print("Green area property updated to nodes: " + str(count))
    print("Total number of nodes in green area: " + str(len(nodes_in_green_area)))
    
    
    greenarea.set_weight(neo4jconn)
    

    
    
if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))
