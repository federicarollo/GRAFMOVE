import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tqdm import tqdm
from ast import operator
from neo4j import GraphDatabase
import overpy
import json
import argparse
import folium as fo
import os
import time
import numpy as np
import pandas as pd
from utils.db_utils import Neo4jConnection
from utils.select_amenity import SelectAmenities
import logging
logging.getLogger("neo4j").setLevel(logging.ERROR)

class PathUtils:
    
    def elem_to_feature(elem, geomType):

        if geomType == "LineString":
            prop = {}
            if("tags" in elem.keys()):
                for key in elem['tags'].keys():
                    prop[key]=elem['tags'][key]
                prop['nodes']=elem['nodes']
                return {
                    "geometry": {
                            "type": geomType,
                            "coordinates": [[d["lon"], d["lat"]] for d in elem["geometry"]]
                    },
                    "properties": prop
                }
        
        if geomType == "Polygon":
            return [(d["lat"], d["lon"]) for d in elem["geometry"]]
        
        return {
            "geometry": {
                "type": geomType,
                "coordinates": [elem["lon"], elem["lat"]]
            },
            "properties": {}
        }
    
    
    def get_coordinates(self,conn,path):
        """evaluate the best route between the source and the target"""
        with conn.driver.session() as session:
            query = """
            unwind %s as p
            match (n:FootNode{id: p}) 
            return collect([n.lat,n.lon])"""%(path)
            result = session.run(query)
            return result.values()
            

    def evaluate_path_metrics(self,conn,path):
        distance = 0
        green_area_weight = 0
        with conn.driver.session() as session:
            for index in range(len(path)-1):
                start_node = path[index]
                end_node = path[index+1]
                
                query = """
                match (:FootNode{id: '%s'})-[r:ROUTE]-(:FootNode{id: '%s'})
                return r.distance as distance, r.green_area_weight as green_area_weight"""%(start_node,end_node)
                result = session.run(query)
                
                values = result.values()[0]
                
                distance+=values[0]
                green_area_weight+=values[1]
                
        return distance, green_area_weight



def add_options():
    parser = argparse.ArgumentParser(description='Visualize path')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--points', '-ps', dest='points', type=str,
                       help="""Insert space-separated OSM identifiers of the FootNode nodes, if no value is specified all the amenities in the graph are extracted, if the values is 'bbox' the amenities in the specified bounding box are extracted.""",
                       required=True)
    parser.add_argument('--map_filename', '-map', dest='map_filename', type=str,
                        help="""Insert the name of the file to draw the map with the paths.""",
                        required=False, default="map.html")
    return parser



def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    neo4jconn = Neo4jConnection(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    neo4jconn.open_connection()
    
    path_utils = PathUtils()
    
    points = options.points.split()
    points = [str(p) for p in points]

    coordinates = path_utils.get_coordinates(neo4jconn, points)
    
    m = fo.Map(location=[coordinates[0][0][0][0], coordinates[0][0][0][1]], zoom_start=13)
    if len(coordinates[0][0]) == 0:
            print('No path')
    else:
        fo.PolyLine(coordinates[0][0], color="green", weight=5).add_to(m)
        m.save(options.map_filename)

    
    distance, green_area_weight = path_utils.evaluate_path_metrics(neo4jconn, points)
    print("Distance: " + str(distance))
    print("Green area weight: " + str(green_area_weight))
    
    neo4jconn.close_connection()
    
    return 0


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))