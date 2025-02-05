from neo4j import GraphDatabase
import argparse
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import json
from shapely import wkt
from neo4j import GraphDatabase
import json
import argparse
import os
import geopandas as gpd
import pandas as pd
import requests
from Tools import save_gdf
from Tools import elem_to_feature
import geojson


"""In this file we are going to make some preprocessing in order to find
   relations between pedestrian paths
"""


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_path(self):
        """gets the path of the neo4j instance"""

        with self.driver.session() as session:
            result = session.write_transaction(self._get_path)
            return result

    @staticmethod
    def _get_path(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.neo4j_home' return value;
                    """)
        return result.values()
        
    def get_import_folder_name(self):
        """gets the path of the import folder of the neo4j instance"""

        with self.driver.session() as session:
            result = session.write_transaction(self._get_import_folder_name)
            return result

    @staticmethod
    def _get_import_folder_name(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.import' return value;
                    """)
        return result.values()
    
    def find_footnodes(self):
        """import Footway nodes on a Neo4j Spatial Layer"""
        with self.driver.session() as session:
            result = session.write_transaction(self._find_footnodes)
        return result

    @staticmethod
    def _find_footnodes(tx):
        result = tx.run("""
                        match (n:FootNode)  
                        return distinct n.id
                """)        
        return result.values()
    
'''
    def find_matching_footnodes(self, footnodes_list):
        """import Footway nodes on a Neo4j Spatial Layer"""
        with self.driver.session() as session:
            result = session.write_transaction(self._find_matching_footnodes, footnodes_list)
        return result

# with distinct nodo match(f:FootNode) where f.id = toString(nodo)

    @staticmethod
    def _find_matching_footnodes(tx, footnodes_list):
        result = tx.run("""
                        with $footnodes_list as footnodes_list unwind footnodes_list as f
                        with f match(n:FootNode) where n.id = toString(f) 
                        return n
                """, footnodes_list=footnodes_list)        
        return result.values()
  
    def check_nodes(self, id, nodes_list):
        """import Footway nodes on a Neo4j Spatial Layer"""
        with self.driver.session() as session:
            result = session.write_transaction(self._find_matching_footnodes, id, nodes_list)
        return result
# with distinct nodo match(f:FootNode) where f.id = toString(nodo)

    @staticmethod
    def _find_matching_footnodes(tx, id, nodes_list):
        result = tx.run("""
                        match (f:Footway) where f.osm_id = $ idwith $footnodes_list as footnodes_list unwind footnodes_list as f
                        with f match(n:FootNode) where n.id = toString(f) 
                        return n
                """, footnodes_list=footnodes_list)        
        return result.values()
'''

def add_options():
    """parameters to be used in order to run the script"""

    parser = argparse.ArgumentParser(description='Data elaboration of footways.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    '''
    parser.add_argument('--nameFile1', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .json file.""",
                        required=True)
    
    parser.add_argument('--nameFile2', '-f2', dest='file_name2', type=str,
                        help="""Insert the name of the .json file.""",
                        required=True)
    '''
    return parser

def createQueryFootNodes(nodes):
    """Create the query to fetch the data of interest"""
    query = f"""[out:json];(node(id:{nodes})->.all;);out body;
            """
    return query


def main(args=None):

    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    #file = options.file_name
    
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'
    url = 'http://overpass-api.de/api/interpreter'
    '''
    f = open(path + file)
    footways = json.load(f) #missing_footways.json
    #lista dei footnode già nel db
    existing_footnodes_list = greeter.find_footnodes()
    existing_footnodes = []
    for i in range(len(existing_footnodes_list)):
        existing_footnodes.append(existing_footnodes_list[i][0]) 
    #voglio sapere quali footnode delle nuove footways mancano
    missing = []
    for i in footways['data']:
        for n in i['nodes']:
            if str(n) not in existing_footnodes:
                missing.append(n)
    missing = list(set(missing))
    f.close()
    #devo creare i nodi mancanti interrogando OSM
    with open("missing_footnodes_list.txt", "w") as o:
        for n in missing:
            o.write(str(n)+", ")
    '''
    #fatto direttamente da overpass turbo perchè da qui ci mette troppo tempo --> missing_footnodes.geojson
    '''
    query = createQueryFootNodes(missing)
    result = requests.get(url, params={'data': query})
    data = result.json()['elements']
    features = [elem_to_feature(elem, "Point") for elem in data]
    gdf = gpd.GeoDataFrame.from_features(features, crs=4326)
    list_ids = [str(elem["id"]) for elem in data]
    gdf.insert(0, 'id', list_ids)    
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'
    save_gdf(gdf, path, "missing_footnodes.json")
    print("Storing footnodes: done")
    '''
    
    
    j=  open("missing_footnodes.geojson") 
    data = geojson.load(j)
    nodes = data['features']
    features = [elem_to_feature(elem, "Point") for elem in nodes]
    gdf = gpd.GeoDataFrame.from_features(features, crs=4326)
    list_ids = [str(elem["id"]).replace("node/","") for elem in nodes]
    gdf.insert(0, 'id', list_ids)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'
    save_gdf(gdf, path, "missing_footnodes.json")
    print("Storing footnodes: done")
    
    f = open(path + "missing_footnodes.json")
    footnodes = json.load(f)
    f.close()
    for n in footnodes['data']:
        g = n['geometry']
        g = g.replace("POINT (", "")
        g = g.replace(")", "")
        coords = g.split(" ")
        n['x'] = coords[0]
        n['y'] = coords[1]
        n['lat'] = float(n['y'])
        n['lon'] = float(n['x'])
        n['geometry'] = "POINT ({} {})".format(n['x'],n['y'])
        #n['location'] = point({latitude: tofloat(n.y), longitude: tofloat(n.x)})
       
    
    with open(path + "missing_footnodes.json", "w") as p:
        json.dump(footnodes,p)


if __name__ == "__main__":
    main()

