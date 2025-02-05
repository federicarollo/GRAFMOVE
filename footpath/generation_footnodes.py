

from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to show how to generate nodes referring to footways"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()


    def import_footnodes(self, file):
        """Import footnodes data on Neo4j and generate Footway nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_footnodes, file)
            return result

    @staticmethod
    def _import_footnodes(tx, file):
        result = tx.run("""
                        call apoc.load.json($file) yield value as value with value.data as data unwind data as record 
                        MERGE (n:FootNode {id : record.id}) 
                        ON CREATE SET n.geometry = record.geometry,
                        n.lat = record.lat, n.lon = record.lon,
                        n.x = record.x, n.y = record.y,
                        n.location = point({latitude: tofloat(record.y), longitude: tofloat(record.x)});
                """, file=file)

        return result.values()



    

def add_options():
    """Paramters needed to run the script"""
    parser = argparse.ArgumentParser(description='Insertion of footways in the graph.')
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
                        help="""Insert the name of the .json file.""",
                        required=True)
    return parser

def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Import footnodes data on Neo4j and generate Footway nodes"""
    start_time = time.time()
    res = greeter.import_footnodes(options.file_name)
    #print(res)
    print("import footnodes: done")
    print("Execution time : %s seconds" % (time.time() - start_time))



if __name__ == "__main__":
    main()