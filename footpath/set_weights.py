from ast import operator
from neo4j import GraphDatabase
import overpy
import json
import argparse
import folium as fo
import os
import time

"""In this file we are going to show how to set weights on subgraphs' relationships"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_footways_ids(self):
        """Get footways ids"""
        with self.driver.session() as session:
            result = session.write_transaction(self._get_footways_ids)
            return result
    @staticmethod
    def _get_footways_ids(tx):    
        result = tx.run("""
               MATCH (n:Footway)-[:CONTAINS]->(f:FootNode) where f.green_area = 0.2 return distinct n.osm_id 
                """)
        return result.values()

    def match_green_area_nodes(self, id):
        """Get footways ids"""
        with self.driver.session() as session:
            result = session.write_transaction(self._match_green_area_nodes, id)
            return result
    @staticmethod
    def _match_green_area_nodes(tx, id):    
        result = tx.run("""
               match (f:Footway) where f.osm_id = $id match(f)-[:CONTAINS]->(fn:FootNode) where fn.green_area = 0.2 return count(fn) as green_nodes
                """, id=id)
        return result.values()
    
    def set_green_area(self, id, green_nodes):
        """Get footways ids"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_green_area, id, green_nodes)
            return result
    @staticmethod
    def _set_green_area(tx, id, green_nodes):    
        result = tx.run("""
               match (f:Footway) where f.osm_id = $id  set f.green_area =  $green_nodes*1.0/size(f.nodes) return f.green_area
                """, id=id, green_nodes = green_nodes)
        return result.values()

    def set_relations_weights(self, beta):
        """Set weights on subgraphs' relationships"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_relations_weights)
            return result

    @staticmethod
    def _set_relations_weights(tx):    
        result = tx.run("""
               match (f:Footway)-[:CONTAINS]->(fn:FootNode)
               set fn.smoothness = f.smoothness, fn.surface = f.surface, fn.incline = f.incline, fn.bridge = f.bridge
                """)
        #AGGIUNGERE A FOOTWAY ATTRIBUTO GREEN_AREA IN PERCENTUALE RISPETTO AL NUMERO DI FOOTNODE
        #CONTROLLARE VALORI PERCENTUALI COME METTERLI 
        #exellent deve facilitare --> se -0.05 mi fa calare il costo
        tx.run("""
                match(n:FootNode) where n.smoothness = "excellent" set n.smoothness = -0.05
                """)
        tx.run("""
               match(n1:FootNode) where n1.smoothness = "good" set n1.smoothness = 0
                """)
        tx.run("""
               match(n2:FootNode) where n2.smoothness = "intermediate" set n2.smoothness = 0.1
                """)
        tx.run("""
               match(n3:FootNode) where n3.smoothness = "bad" set n3.smoothness = 0.2;
                """)
        tx.run("""
               match(n:FootNode) where n.smoothness is null set n.smoothness = 0;
                """)
        tx.run("""
                match(n:FootNode) where n.surface = "asphalt" or n.surface = "concrete" or n.surface = "concrete:plates" or n.surface = "compacted" or n.surface = "paved" set n.surface = 0
                """)
        tx.run("""
               match(n:FootNode) where n.surface = "grass_paver" or n.surface = "sett" or n.surface = "unhewn_cobblestone" or n.surface = "grass" or n.surface = "cobblestone" or n.surface = "paving_stones" set n.surface = 0.1
                """)
        tx.run("""
               match(n:FootNode) where n.surface = "unpaved" or n.surface = "rock"  or n.surface = "wood"  or n.surface = "gravel"  or n.surface = "fine_gravel" or n.surface = "pebblestone"  or n.surface = "ground" or n.surface = "dirt" set n.surface = 0.2;
                """)
        tx.run("""
                match(n:FootNode) where n.surface is null set n.surface = 0
                """)
        tx.run("""
                match(n:FootNode) where n.incline = "yes" set n.incline = 0.1
                """)
        tx.run("""
               match(n:FootNode) where n.incline <>0.1 and n.incline is not null set n.incline = 0
                """)
        tx.run("""
                match(n:FootNode) where n.incline is null set n.incline = 0
                """)
        tx.run("""
               match(n:FootNode) where n.bridge = "yes" set n.bridge = 0.1
                """)
        tx.run("""
               match(n:FootNode) where n.bridge is null set n.smoothness = 0;
                """)
        tx.run("""
                match(n:FootNode) where n.green_area = "yes" set n.green_area = 0.2
                """)
        tx.run("""
               match(n:FootNode) where n.green_area is null set n.green_area = 0;
                """)
        tx.run("""match (f:Footway) 
                  where not f.highway in ['path','pedestrian','footway','track','steps'] 
                  and not "BicycleLane" in labels(f) 
                  set f.danger = 3""")
        tx.run("""match (f:Footway) 
                  where f.highway in ['path','pedestrian','footway','track','steps']
                  and not "BicycleLane" in labels(f)
                  set f.danger = 1""")
        tx.run("""
                match (b:FootNode)-[r:FOOT_ROUTE]-(b2:FootNode) 
                match (b)<-[:CONTAINS]-(bl:Footway)-[:CONTAINS]->(b2)
                set r.danger = bl.danger;
                """)
        tx.run("""
                match (b:FootNode)-[r:FOOT_ROUTE]-(b2:FootNode) 
                match (b)<-[:CONTAINS]-(bl:Footway)-[:CONTINUE_ON_FOOTWAY]-(bl2:Footway)-[:CONTAINS]->(b2)
                where bl.osm_id <> bl2.osm_id
                set r.danger = round(toFloat(bl.danger + bl2.danger)/2,0,'UP')""")
        tx.run("""
                MATCH(n)-[r:FOOT_ROUTE]->(n1) set r.speed = 4;
                """)
        tx.run("""
                MATCH(n)-[r:FOOT_ROUTE]->(n1) set r.distance = distance(n.location, n1.location);
                """)
        tx.run("""
                MATCH(n1)-[r:FOOT_ROUTE]->(n2) set r.travel_time = (r.distance * 3.6) /r.speed;                
                """)
        tx.run("""
                MATCH(n1)-[r:FOOT_ROUTE]->(n2) set r.min_travel_time = (r.distance * 3.6) /6;                
                """)
        tx.run("""
                MATCH(n1)-[r:FOOT_ROUTE]->(n2) set r.max_travel_time = (r.distance * 3.6) /2;                
                """)
        tx.run("""
                MATCH(n1)-[r:FOOT_ROUTE]-(n2) set r.cost = 0.5*((r.travel_time-r.min_travel_time)/(r.max_travel_time-r.min_travel_time))+0.5*((r.danger-1)/4);                
                """)
        tx.run("""
                MATCH(n1)-[r:FOOT_ROUTE]-(n2) set r.comfort_cost = 0.5*r.cost*(2-n1.green_area-n2.green_area+n1.surface+n2.surface+n1.smoothness+n2.smoothness+n1.incline+n2.incline+n1.bridge+n2.bridge);                
                """)
       
        return result.values()



    

def add_options():
    """Parameters needed to run the script"""
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
    parser.add_argument('--beta', '-b', dest='beta', type=float,
                        help="""Insert the beta parameter between 0 and 1. The value represent the importance of travel time on the final cost.""",
                        required=False, default = 0.5)
    return parser


def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    if(options.beta > 1 or options.beta < 0):
        print("The beta parameter value is not valid, 0.5 will be used")
        options.beta = 0.5

    """Set weights on subgraphs' relationshps"""
    greeter.set_relations_weights(options.beta)
    #coords = greeter.get_coords()
    print("Setting the relationships weight for the routing : done")
    "Look for footways connected to green_area footnodes and set property green_area of footways"
    ids = greeter.get_footways_ids()
    for id in ids:
        #print(id[0])
        green_nodes = greeter.match_green_area_nodes(id[0])[0][0]
        #print(green_nodes)
        green_area = greeter.set_green_area(id[0], green_nodes)[0][0]
        #print(green_area)
    return 0


if __name__ == "__main__":
    main()
