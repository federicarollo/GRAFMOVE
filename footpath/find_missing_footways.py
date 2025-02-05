from neo4j import GraphDatabase
import argparse
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import json
from shapely import wkt

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
    
    def find_matching_footways(self, file):
        """import Footway nodes on a Neo4j Spatial Layer"""
        with self.driver.session() as session:
            result = session.write_transaction(self._find_matching_footways, file)
        return result

    @staticmethod
    def _find_matching_footways(tx, file):
        result = tx.run("""
                        call apoc.load.json($file) yield value as value with value.data as data 
                        unwind data as record match(n:Footway{osm_id : record.id}) return record.id
                """, file=file)        
        return result.values()


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
    parser.add_argument('--nameFile1', '-f1', dest='file_name1', type=str,
                        help="""Insert the name of the .json file.""",
                        required=True)
    parser.add_argument('--nameFile2', '-f2', dest='file_name2', type=str,
                        help="""Insert the name of the .json file.""",
                        required=True)
    return parser




def main(args=None):

    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    file1 = options.file_name1
    file2 = options.file_name2
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'

    f1 = open(path + file1) #footways_from_OSM.json
    f2 = open(path + file2) #footways_near_parks.json
    footways1 = json.load(f1)
    footways2 = json.load(f2)
    result1 = greeter.find_matching_footways(file1)
    result2 = greeter.find_matching_footways(file2)
    f1.close()
    f2.close()
    matching_footways_ids = []
    for e in range(len(result1)):
        matching_footways_ids.append(result1[e][0])
    for e in range(len(result2)):
        matching_footways_ids.append(result2[e][0])
    matching_footways_ids = list(set(matching_footways_ids)) 
    #tutti gli indici senza duplicati delle footways che matchano nel db
        
    indx = [] #tutti gli indici senza duplicati delle footways nei file
    for i in footways1['data']:
        indx.append(i['id'])
    for i in footways2['data']:
        indx.append(i['id'])
    indx = list(set(indx))
    #print(len(indx)) 32445
    #print(len(matching_footways_ids)) 2721
    non_matching_idx = [x for x in indx if x not in matching_footways_ids] #indici nel file MA che NON hanno match nel db
    #print(len(non_matching_idx)) 524
    difference = []
    for j in footways2["data"]:
        not_in = 1
        for i in footways1["data"]:
            if(j['id'] == i["id"]):
                not_in=0
        if not_in == 1:
            difference.append(j['id']) #footways solo in near_parks
    #print(len(difference)) 140
    non_matching = []
    #count = 0
    for i in non_matching_idx:
        for f1 in footways1['data']:
            if f1['id']==i:
                non_matching.append(f1)
    #            count +=1
    
    still_missing = [x for x in difference if x in non_matching_idx]
    #print(len(still_missing)) 140
    for i in still_missing:
        for f2 in footways2['data']:
            if f2['id']==i:
                non_matching.append(f2)
                #count +=1
    #print(count) 524
    non_matching_dict = {}
    non_matching_dict["data"] = non_matching
    print(len(non_matching))
    with open(path + "missing_footways.json","w") as o:
        json.dump(non_matching_dict, o)



if __name__ == "__main__":
    main()