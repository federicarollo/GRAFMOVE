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


    def import_footways(self, file):
        """Import footways data on Neo4j and generate Footway nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._import_footways, file)
            return result

    @staticmethod
    def _import_footways(tx, file):
        result = tx.run("""
                        call apoc.load.json($file) yield value as value with value.data as data unwind data as record 
                        MATCH (n:BicycleLane {osm_id : record.id}) 
                        SET n:Footway, 
                        n.touched_footways = record.touched_footways,n.lit = record.lit, n.smoothness = record.smoothness, n.surface = record.surface, n.incline = record.incline,
                        n.bridge=record.bridge;
                """, file=file)
        tx.run("""
                        call apoc.load.json($file) yield value as value with value.data as data unwind data as record 
                        MERGE (n:Footway {osm_id : record.id}) 
                        ON CREATE SET n.geometry = record.geometry, n.touched_lanes = record.touched_lanes, 
                        n.touched_footways = record.touched_footways,
                        n.nodes = record.nodes,
                        n.bicycle=record.bicycle, n.bus=record.bus, n.crossing=record.crossing, 
                        n.cycleway=record.cycleway, n.kerb=record.kerb, n.length = record.length, n.highway = record.highway,
                       n.lit = record.lit, n.smoothness = record.smoothness, n.surface = record.surface, n.incline = record.incline,
                        n.bridge=record.bridge;
                """, file=file)

        return result.values()
    
    def add_info_existing_footways(self, file):
        """Import footways data on Neo4j and generate Footway nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._add_info_existing_footways, file)
            return result

    @staticmethod
    def _add_info_existing_footways(tx, file):
        result = tx.run("""
                        call apoc.load.json($file) yield value as value with value.data as data unwind data as record 
                        MATCH (n:Footway {osm_id : record.id}) 
                        SET n.touched_footways = record.touched_footways,n.lit = record.lit, n.smoothness = record.smoothness, n.surface = record.surface, n.incline = record.incline,
                        n.bridge=record.bridge,  n.nodes = record.nodes, n.geometry = record.geometry;
                """, file=file)
        return result.values()

    def generate_relationships_touched_footways(self):
        """Generate relationships between nodes representing footways that touch or intersect"""
        with self.driver.session() as session:
            result = session.write_transaction(self._generate_relationships_touched_footways)
            return result

    @staticmethod
    def _generate_relationships_touched_footways(tx):
        result = tx.run("""
                match(b:Footway) where NOT isEmpty(b.touched_footways) unwind b.touched_footways as f match (b1:Footway) 
                where b1.osm_id = f and b.geometry <> b1.geometry
                merge (b)-[r:CONTINUE_ON_FOOTWAY]->(b1)
                merge (b1)-[r1:CONTINUE_ON_FOOTWAY]->(b)
        """)
        return result

    def generate_relationships_closest_footways(self, file):
        """Generate relationships between nodes representing footways reachable by crossing the road where the
           crossing is not signaled
        """
        with self.driver.session() as session:
            result = session.write_transaction(self._generate_relationships_closest_footways, file)
            return result

    
    @staticmethod
    def _generate_relationships_closest_footways(tx, file):
        result = tx.run("""
                call apoc.load.json($file) yield value as value with value.data as data 
                UNWIND data as record match (f:Footway) where f.osm_id = record.id and NOT isEmpty(record.closest_footways)
                UNWIND record.closest_footways as foot with f, foot match (f1:Footway) where f1.osm_id = foot[0] and f.osm_id <> f1.osm_id
                merge (f)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(f1) on create set r.length = foot[1]
                merge (f1)-[r1:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]->(f) on create set r1.length = foot[1];
        """, file=file)

        return result

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
                        help="""Insert the name of the missing_footways.json file.""",
                        required=True)
    parser.add_argument('--nameFile2', '-f2', dest='file_name2', type=str,
                        help="""Insert the name of the footways_from_OSM.json file.""",
                        required=True)
    parser.add_argument('--nameFile3', '-f3', dest='file_name3', type=str,
                        help="""Insert the name of the footways_near_parks.json file.""",
                        required=True)
    return parser

def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Import footways data on Neo4j and generate Footway nodes + add info to existing footways"""
    start_time = time.time()
    greeter.import_footways(options.file_name)
    print("import missing_footways.json: done")
    greeter.add_info_existing_footways(options.file_name2)
    greeter.add_info_existing_footways(options.file_name3)
    print("adding info to existing footways: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    """Generate relationships between nodes representing footways that touch or intersect"""
    start_time = time.time()
    greeter.generate_relationships_touched_footways()
    print("Connect the footways that touches each other: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    """Generate relationships between nodes representing footways reachable by crossing the road where the 
       crossing is not signaled
    """
    start_time = time.time()
    greeter.generate_relationships_closest_footways(options.file_name)
    print("Connect the footways that are close to each other: done")
    print("Execution time : %s seconds" % (time.time() - start_time))


if __name__ == "__main__":
    main()