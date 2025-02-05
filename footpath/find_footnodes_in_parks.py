from neo4j import GraphDatabase
import json
import argparse
import os
import geopandas as gpd
import pandas as pd
import requests
from Tools import save_gdf
from Tools import elem_to_feature

class App:
    """In this file we are going to extract footways from OSM"""

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
    
    def find_matching_footnodes(self, footnodes_list):
        with self.driver.session() as session:
            result = session.write_transaction(self._find_matching_footnodes, footnodes_list)
        return result

    @staticmethod
    def _find_matching_footnodes(tx, footnodes_list):
        result = tx.run("""
                        with $footnodes_list as footnodes_list unwind footnodes_list as f
                        with f match(n:FootNode) where n.id = toString(f) 
                        set n.green_area = \"yes\" return count(n)
                """, footnodes_list=footnodes_list)        
        return result.values()


def add_options():
    """parameters to be used in order to run the script"""

    parser = argparse.ArgumentParser(description='Insertion of CROSSING NODES in the graph.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str, default='neo4j://localhost:7687',
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str, default='neo4j',
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str, default='TesinaGA',
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--latitude', '-x', dest='lat', type=float,default='44.645885',
                        help="""Insert latitude of city center""",
                        required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,default='10.9255707',
                        help="""Insert longitude of city center""",
                        required=True)
    parser.add_argument('--distance', '-d', dest='dist', type=float,default='5000',
                        help="""Insert distance (in meters) of the area to be covered""",
                        required=True)
    return parser



def createQueryParks(dist, lat, lon):
    """Create the query to fetch the data of interest"""

    query = f"""[out:json];
                            (
                            way["leisure"="park"](around:{dist},{lat},{lon});
                            );
                            out geom;
                           """
    return query

def createQueryNodesInParks(poly):
    """Create the query to fetch the data of interest"""

    query = f"""[out:json];
                            (
                            node(poly:\"{poly}\");
                            );
                            out geom;
                           """
    return query


def main(args=None):

    """Parsing of input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    dist = options.dist
    lon = options.lon
    lat = options.lat
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    url = 'http://overpass-api.de/api/interpreter'

    '''ottengo lista di liste di tuple (coordinate dei punti del parco --> i suoi border)
    mi servono per definire il filtro poligono'''
    query = createQueryParks(dist, lat, lon)
    result = requests.get(url, params={'data': query})
    data = result.json()['elements']
    features = [elem_to_feature(elem, "Polygon") for elem in data]
  
    '''creo lista di stringhe che per ogni parco, contengono i suoi nodi come lat lon separate da spazi'''
    id_nodes_in_parks = []
    #polygons = []
    for i in range(len(features)):
        poly = ''
        for j in range(len(features[i])):
            if j == len(features[i]) -1: #ultimo lon NON deve essere seguito da spazio altrimenti la query non funziona
                poly += str(features[i][j][0]) + " " + str(features[i][j][1])
            else:
                poly += str(features[i][j][0]) + " " + str(features[i][j][1]) + ' '
        #polygons.append(poly)
        query = createQueryNodesInParks(poly)
        #print(query)
        '''query che trova i nodi interni al poligono (che rappresenta il parco)'''
        result = requests.get(url, params={'data': query})
        data = result.json()['elements']
        '''aggiungo a lista gli id dei nodi interni ai parchi trovati'''
        for elem in data:
            id_nodes_in_parks.append(str(elem["id"]))
        #id_nodes_in_parks.append([str(elem["id"]) for elem in data]) #lista di liste ognuna con i footnode interni ad ogni parco
        #print([str(elem["id"]) for elem in data])
    #id_nodes_in_parks = [node for node_list in id_nodes_in_parks for node in node_list]
    #print(len(id_nodes_in_parks)) #9013 nodi
    #print(polygons)
    
    '''inserisci attributo green_area = 'yes' sui footnode trovati'''
    count  = greeter.find_matching_footnodes(id_nodes_in_parks)
    print(count) #7173
    
    return 0

if __name__ == "__main__":
    main()
