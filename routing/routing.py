import sys
import os
from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


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
from footpath_osmnx.selectAmenities import selectAmenities #import select_amenity, select_amenity_in_bbox
import logging
logging.getLogger("neo4j").setLevel(logging.ERROR)

class Routing:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def evaluate_path_metrics(self,pairs):
        """evaluate path metrics
        """
        with self.driver.session() as session:
            result = session.write_transaction(self._evaluate_path_metrics,pairs)
            return result
    @staticmethod
    def _evaluate_path_metrics(tx,pairs):
        query = """unwind %s as pairs
                match (n:RoadJunction{id: pairs[0]})-[r:ROUTE]->(m:RoadJunction{id:pairs[1]})
                with min(r.cost) as min_cost, pairs
                match (n:RoadJunction{id: pairs[0]})-[r:ROUTE]->(m:RoadJunction{id:pairs[1]})
                where r.cost = min_cost
                return sum(r.cost) as cost,avg(r.danger) as danger,sum(r.distance) as distance"""%(pairs)
        result = tx.run(query)
        return result.values()

    def get_coordinates(self,final_path):
        """evaluate the best route between the source and the target
        """
        with self.driver.session() as session:
            result = session.write_transaction(self._get_coordinates,final_path)
            return result
    @staticmethod
    def _get_coordinates(tx,final_path): 
        query = """
        unwind %s as p
        match (n:RoadJunction{id: p}) return collect([n.lat,n.lon])"""%(final_path)
        #print(query)
        result = tx.run(query)
        return result.values()
        
    def drop_all_projections(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._drop_all_projections)
            return result
    @staticmethod    
    def _drop_all_projections(tx):
        result = tx.run("""CALL gds.graph.list() YIELD graphName
                    CALL gds.graph.drop(graphName)
                    YIELD database
                    RETURN 'dropped ' + graphName""")
        return result.values()

    def routing(self, pointA, pointB, weight):
        """evaluate the best route between the source and the target"""
        with self.driver.session() as session:
            query = """
            MATCH (a:RoadJunction {id: '%s'}), (b:RoadJunction {id: '%s'})
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

    # @staticmethod
    # def _routing(tx, pointA, pointB):
    #     # tx.run("""call gds.graph.project('subgraph_routing', ['RoadJunction'], 
    #     #         ['ROUTE'], 
    #     #         {nodeProperties: ['lat', 'lon'], relationshipProperties: ['distance']});
    #     #     """)
    #     query = """
	# 	MATCH (a:RoadJunction {id: '%s'}), (b:RoadJunction {id: '%s'})
    #     CALL apoc.algo.aStar(
    #       a, 
    #       b, 
    #       'ROUTE',
    #       '%s', 'lat', 'lon') 
    #     YIELD path, weight
    #     RETURN         
    #       [r in relationships(path) | id(r)] as shortestHopRelIds,
    #       weight as totalCost, 
    #       [n in nodes(path) | n.id] as shortestHopNodeIds """%(str(pointA), str(pointB), str(weight))
    # 
    #     # print(query)
    # 
    #     result = tx.run(query)
    #     
    #     # tx.run("""call gds.graph.drop('subgraph_routing')""")
    #     return result.values()[0]

    def find_best_path(self, pointA, pointB, weight):
        start_time = time.time()
        result = self.routing(pointA, pointB, weight)
        
        dic = {}
        dic["path"] = result[0]
        dic["cost"] = result[1]
        dic["nodes"] = result[2]
        # print(dic)

        # dic['exec_time']=time.time() - start_time
        # dic['hops']=len(dic["nodes"])

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
                       help="""Insert space-separated OSM identifiers of the RoadJunction nodes, if no value is specified all the amenities in the graph are extracted, if the values is 'bbox' the amenities in the specified bounding box are extracted.""",
                       required=False, default='all')
    parser.add_argument('--weight', '-w', dest='weight', type=str,
                       help="""Insert the weight to optimize.""",
                       required=True, default="distance")
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
                        help="""Insert the name of the file to write the paths (as sequence of RoadJunction nodes).""",
                        required=False, default="paths.csv")
    return parser


# python routing.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd footpath_osmnx_also_private --points bbox
# python routing/routing.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd footpath_osmnx_also_private --points bbox --latitude_min 44.640049 --latitude_max 44.652324 --longitude_min 10.917066 --longitude_max 10.934938 --weight green_area_weight
# python routing/routing.py --neo4jURL neo4j://localhost:7687 --neo4juser neo4j  --neo4jpwd footpath_osmnx_also_private --weight green_area_weight

def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    routing = Routing(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    
    if(options.points == 'all' or options.points == 'bbox'):
        sa = selectAmenities(options.neo4jURL, options.neo4juser, options.neo4jpwd)
        amenities = sa.select_amenity()
        if(options.points == 'bbox'):
            amenities = sa.select_amenity_in_bbox(amenities, options.latitude_min, options.latitude_max, options.longitude_min, options.longitude_max)
        else:
            amenities = sa.amenity_to_df(amenities)
        points = amenities['rj_osm_id'].values
        print(amenities)
    else:
        points = options.points.split()
        points = [str(p) for p in points]
    
    # mapFilename = options.mapName
    matrixFilename = options.matrix_filename
    pathFilename = options.path_filename
    
    weight = options.weight
    
    points = np.unique(points)
    n_points = len(points)
    print("Number of points: " + str(n_points))
    
    
    # m = fo.Map(location=[44.646388, 10.926560], zoom_start=13)
    

    with routing.driver.session() as session:
        query = """call gds.graph.project(
        'subgraph_routing', 
        ['RoadJunction'], 
        ['ROUTE'], 
        {nodeProperties: ['lat', 'lon'], relationshipProperties: ['%s']})"""%(str(weight))
        session.run(query)
    
    """
    for index_row in range(n_points):
        for index_column in range(n_points):
            if(index_row!=index_column) and (distance_matrix[index_column][index_row]==0):
                best_path = find_best_path(greeter, points[index_row], points[index_column])
        
                distance_matrix[index_row][index_column] = best_path['cost']
                distance_matrix[index_column][index_row] = best_path['cost']
                
                coordinates = greeter.get_coordinates(final_path = str(best_path['nodes']))
                # print(coordinates)
                
                fo.PolyLine(coordinates[0][0], color="green", weight=5).add_to(m)

    distance_matrix_df = pd.DataFrame(distance_matrix, index=points, columns=points)
    distance_matrix_df.to_csv(matrixFilename)
    
    fo.LayerControl().add_to(m)
    m.save(mapFilename + '.html')
    """
    
    weight_matrix = np.zeros([n_points, n_points])
    paths = []
    
    for index_row in tqdm(range(n_points), desc="Rows"):
        for index_column in range(index_row + 1, n_points):
            if weight_matrix[index_row][index_column] == 0:
                best_path = routing.find_best_path(points[index_row], points[index_column], weight)
                cost = best_path['cost']
                nodes = best_path['nodes']
                
                weight_matrix[index_row][index_column] = cost
                weight_matrix[index_column][index_row] = cost
                
                paths.append({
                    'start_point': points[index_row],
                    'end_point': points[index_column],
                    'cost': cost,
                    'nodes': ' '.join(nodes)
                })
            
    
    weight_matrix_df = pd.DataFrame(weight_matrix, index=points, columns=points)
    # weight_matrix_df = df.apply(lambda row: row.map(lambda x: find_best_path(greeter, row.name, row.index[row.name], weight)), axis=1)
    weight_matrix_df.to_csv(matrixFilename)
    
    path_df = pd.DataFrame(paths)
    path_df.to_csv(pathFilename, index=False)

    with routing.driver.session() as session:
        query = """CALL gds.graph.drop('subgraph_routing')"""
        session.run(query)
        
    routing.close()
    
    
    """
    if (boolMap):
        #visualization of the path
        print(str(final_path))
        coordinates = greeter.get_coordinates(final_path = str(final_path))
        print(coordinates)
        m = fo.Map(location=[coordinates[0][0][0][0], coordinates[0][0][0][1]], zoom_start=13)
        if len(coordinates[0][0]) == 0:
                print('\nNo result for query')
        else:
            fo.PolyLine(coordinates[0][0], color="green", weight=5).add_to(m)
            m.save(file + '.html')
    """
    
    return 0


if __name__ == "__main__":
    main()

