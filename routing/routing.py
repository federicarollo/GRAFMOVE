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
import time
import numpy as np
import pandas as pd
from utils.db_utils import Neo4jConnection
from utils.select_amenity import SelectAmenities
import logging
logging.getLogger("neo4j").setLevel(logging.ERROR)


class Routing:

    def evaluate_path_metrics(self,conn,pairs):
        with conn.driver.session() as session:
            query = """unwind %s as pairs
                match (n:FootNode{id: pairs[0]})-[r:ROUTE]->(m:FootNode{id:pairs[1]})
                with min(r.cost) as min_cost, pairs
                match (n:FootNode{id: pairs[0]})-[r:ROUTE]->(m:FootNode{id:pairs[1]})
                where r.cost = min_cost
                return sum(r.cost) as cost,avg(r.danger) as danger,sum(r.distance) as distance"""%(pairs)
            result = session.run(query)
        return result.values()

    def routing(self, conn, pointA, pointB, weight):
        """evaluate the best route between the source and the target"""
        with conn.driver.session() as session:
            query = """
            MATCH (a:FootNode {id: '%s'}), (b:FootNode {id: '%s'})
            CALL apoc.algo.aStar(
              a, 
              b, 
              'ROUTE',
              '%s', 'lat', 'lon') 
            YIELD path, weight 
            RETURN         
              [r in relationships(path) | id(r)] as shortestHopRelIds,
              weight as totalCost, 
              [n in nodes(path) | n.id] as shortestHopNodeIds """%(str(pointA), str(pointB), str(weight))
            result = session.run(query)
            return result.values()[0]


    def find_best_path(self, conn, pointA, pointB, weight):
        result = self.routing(conn, pointA, pointB, weight)
        
        dic = {}
        dic["path"] = result[0]
        dic["cost"] = result[1]
        dic["nodes"] = result[2]

        return dic

def add_options():
    """Parameters needed to run the script"""
    parser = argparse.ArgumentParser(description='Find the best path between each pair of points and calculate the weight matrix.')
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
                       required=False, default='all')
    parser.add_argument('--weight', '-w', dest='weight', type=str,
                       help="""Insert the weight to optimize.""",
                       required=False, default="distance")
    parser.add_argument('--latitude_min', '-latmin', dest='latitude_min', type=float,
                       help="""The minimum latitude of the bounding box.""",
                       required=False)
    parser.add_argument('--latitude_max', '-latmax', dest='latitude_max', type=float,
                       help="""The maximum latitude of the bounding box.""",
                       required=False)
    parser.add_argument('--longitude_min', '-lonmin', dest='longitude_min', type=float,
                       help="""The minimum longitude of the bounding box.""",
                       required=False)
    parser.add_argument('--longitude_max', '-lonmax', dest='longitude_max', type=float,
                       help="""The maximum longitude of the bounding box.""",
                       required=False)
    # parser.add_argument('--mapName', '-mn', dest='mapName', type=str,
    #                    help="""Insert the name of the file containing the map with the computed path.""",
    #                    required=False, default="map")
    parser.add_argument('--matrix_filename', '-mfn', dest='matrix_filename', type=str,
                        help="""Insert the name of the file to write the weight matrix.""",
                        required=False, default="weight_matrix.csv")
    parser.add_argument('--path_filename', '-pfn', dest='path_filename', type=str,
                        help="""Insert the name of the file to write the paths (as sequence of FootNode nodes).""",
                        required=False, default="paths.csv")
    return parser



def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    neo4jconn = Neo4jConnection(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    neo4jconn.open_connection()
    
    routing = Routing()
    
    if(options.points == 'all' or options.points == 'bbox'):
        sa = SelectAmenities()
        amenities = sa.select_amenity(neo4jconn)
        if(options.points == 'bbox'):
            amenities = sa.select_amenity_in_bbox(amenities, options.latitude_min, options.latitude_max, options.longitude_min, options.longitude_max)
        else:
            amenities = sa.amenity_to_df(amenities)
        points = amenities['rj_osm_id'].values
        print(amenities)
    else:
        points = options.points.split()
        points = [str(p) for p in points]
    
    matrixFilename = options.matrix_filename
    pathFilename = options.path_filename
    
    weight = options.weight
    
    points = np.unique(points)
    n_points = len(points)
    print("Number of points for routing: " + str(n_points))
    
    # with neo4jconn.driver.session() as session:
    #     query = """call gds.graph.project(
    #     'subgraph_routing', 
    #     ['FootNode'], 
    #     ['ROUTE'], 
    #     {nodeProperties: ['lat', 'lon'], relationshipProperties: ['%s']})"""%(str(weight))
    #     session.run(query)
    
    weight_matrix = np.zeros([n_points, n_points])
    paths = []
    
    for index_row in tqdm(range(n_points), desc="Rows"):
        for index_column in range(index_row + 1, n_points):
            if weight_matrix[index_row][index_column] == 0:
                best_path = routing.find_best_path(neo4jconn, points[index_row], points[index_column], weight)
                cost = best_path['cost']
                nodes = best_path['nodes']
                
                weight_matrix[index_row][index_column] = cost
                weight_matrix[index_column][index_row] = cost
                
                paths.append({
                    'start_point': points[index_row],
                    'end_point': points[index_column],
                    'cost': cost,
                    'path_nodes': ' '.join(nodes)
                })
            
    
    weight_matrix_df = pd.DataFrame(weight_matrix, index=points, columns=points)
    weight_matrix_df.to_csv(matrixFilename)
    
    path_df = pd.DataFrame(paths)
    path_df.to_csv(pathFilename, index=False)

    # with neo4jconn.driver.session() as session:
    #     query = """CALL gds.graph.drop('subgraph_routing')"""
    #     session.run(query)

    neo4jconn.close_connection()
    
    return 0


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))