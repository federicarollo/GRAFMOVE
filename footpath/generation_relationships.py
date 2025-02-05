from ast import operator
from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to show how subgraph footways layer nodes are generated"""

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

    def generation_contains(self, file):
        """Import footnodes data on Neo4j and generate FootNodes nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._generation_contains, file)
            return result

    @staticmethod
    def _generation_contains(tx, file):
        result = tx.run("""
                        call apoc.load.json($file) yield value as value with value.data as data unwind data as record
                        match(f:Footway) where f.osm_id = record.id unwind f.nodes as nodo match(n:FootNode) where n.id = toString(nodo)
                        with f, n CREATE (f)-[r:CONTAINS]->(n) return count(r);
                """, file=file)

        return result.values()

    def generation_footroute(self, id1, id2):
        """Import footnodes data on Neo4j and generate FootNodes nodes"""
        with self.driver.session() as session:
                result = session.write_transaction(self._generation_footroute, id1, id2)
        return result

    @staticmethod
    def _generation_footroute(tx, id1, id2):
        result = tx.run("""
                        match (f1:FootNode) where f1.id = toString($id1) match (f2:FootNode) where f2.id = toString($id2) 
                        CREATE (f1)-[r:FOOT_ROUTE]->(f2) 
                        CREATE (f2)-[r2:FOOT_ROUTE]->(f1) return count(r);
                """, id1=id1, id2=id2)

        return result
    


def add_options():
    """Parameters nedeed to run the script"""
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
    parser.add_argument('--nameFile', '-f', dest='file_footways', type=str,
                        help="""Insert the name of the .json file.""",
                        required=True)
    return parser


def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    file_footways = options.file_footways #missing_footways.json
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'
    """generazione relazioni CONTAINS tra footways e footnodes"""
    greeter.generation_contains(file_footways)
    
    """generazione relazioni FOOT_ROUTE tra i footnodes della stessa footway"""
    f = open(path + file_footways)
    footways = json.load(f) 
    #prova =  open(path + "prova_footroute.txt", "w")
    for i in footways['data']:
        if len(i['nodes']) == 1 or len(i['nodes']) == 0:
            #prova.write('VUOTA O 1\n')
            break
        print(len(i['nodes']))
        for j in range(0, len(i['nodes'])-1):
            #prova.write(str(i['nodes'][j]) + ' '  + str(i['nodes'][j+1]) + '\n')
            count = greeter.generation_footroute(i['nodes'][j], i['nodes'][j+1])
            print(count)
        #prova.write(i['id'] +'\n')
    f.close()
    #prova.close()
    return 0


if __name__ == "__main__":
    main()

