import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ast import operator
from neo4j import GraphDatabase
import overpy
import json
import argparse
import folium as fo
import time
import random
from utils.db_utils import Neo4jConnection
from utils.path_utils import PathUtils
from utils.select_amenity import SelectAmenities
import numpy as np


class TSP:


    def find_best_path(self,conn,points,weight):
        """evaluate the best route to visit several point"""
        with conn.driver.session() as session:

            query = """
            WITH %s as selection
            MATCH (c:FootNode) 
            WHERE c.id in selection 
            WITH collect(c) as footnodes 
            UNWIND footnodes as c1 
                WITH 
                  c1, 
                  [c in footnodes where c.id > c1.id] as c2s, 
                  footnodes 
                UNWIND c2s as c2 
                    CALL apoc.algo.aStar(
                      c1, 
                      c2, 
                      'ROUTE',
                      '%s', 'lat', 'lon') 
                    YIELD path, weight 
                    WITH 
                      c1, 
                      c2, 
                      weight as totalCost, 
                      [n in nodes(path) | n.id] as shortestHopNodeIds, 
                      [r in relationships(path) | id(r)] as shortestHopRelIds, 
                      footnodes 
                    MERGE (c1) -[r:SHORTEST_ROUTE_TO]- (c2) 
                    SET r.cost = totalCost 
                    SET r.shortestHopNodeIds = shortestHopNodeIds
                    SET r.shortestHopRelIds = shortestHopRelIds 
                    WITH 
                      c1, 
                      c2, 
                      (size(footnodes)-1) as level, 
                      footnodes 
                    CALL apoc.path.expandConfig(
                      c1, 
                      { relationshipFilter: 'SHORTEST_ROUTE_TO', 
                        minLevel: level, 
                        maxLevel: level, 
                        whitelistNodes: footnodes, 
                        terminatorNodes: [c2], 
                        uniqueness: 'NODE_PATH' } ) 
                    YIELD path
                    WITH nodes(path) as orderedFootnodes, 
                    [node in nodes(path) | node.id] as orderedIds,
                    reduce(cost = 0, x in relationships(path) | cost + x.cost) as totalCost, 
                    [r in relationships(path) | r.shortestHopNodeIds] as shortestRouteNodeIds
                    MATCH (:FootNode {id: orderedIds[0]})-[rel:SHORTEST_ROUTE_TO]-(:FootNode {id: reverse(orderedIds)[0]})
                    WITH orderedFootnodes+orderedFootnodes[0] as orderedFootnodes, orderedIds+orderedIds[0] as orderedIds, totalCost+rel.cost as totalCost, shortestRouteNodeIds as shortestRouteNodeIds
                    ORDER BY totalCost LIMIT 1
                    UNWIND range(0, size(orderedFootnodes)) as index 
                        UNWIND shortestRouteNodeIds[index] as shortestHopNodeId 
                            WITH orderedFootnodes, 
                            totalCost,
                            index,
                            CASE 
                                WHEN toString(shortestRouteNodeIds[index][0]) = toString(orderedIds[index])
                                THEN tail(collect(shortestHopNodeId)) 
                                ELSE tail(reverse(collect(shortestHopNodeId))) 
                            END as orderedHopNodeIds 
                            ORDER BY index 
                            UNWIND orderedHopNodeIds as orderedHopNodeId 
                                MATCH (c:FootNode) where c.id = orderedHopNodeId 
                                with 
                                  [c in orderedFootnodes | c.id] as orderedFootnodesIds, 
                                  totalCost,
                                  orderedFootnodes[0].id as first,
                                  orderedFootnodes[size(orderedFootnodes)-2].id as last,
                                  [orderedFootnodes[0].id] + collect(c.id) as footnodeRouteIds
                            MATCH (:FootNode {id: toString(first)})-[r:SHORTEST_ROUTE_TO]-(:FootNode {id: toString(last)})
                            with orderedFootnodesIds, totalCost, footnodeRouteIds, r,
                            CASE 
                                WHEN toString(r.shortestHopNodeIds[0]) = toString(last)
                                THEN tail(REDUCE(s = [], item IN r.shortestHopNodeIds | s + item))
                                ELSE tail(reverse(REDUCE(s = [], item IN r.shortestHopNodeIds | s + item)))
                            END as lastpath
                            return orderedFootnodesIds, totalCost, footnodeRouteIds+lastpath as totalPath"""%(points.tolist(),weight)

            result = session.run(query)
            return result.values()[0]


def add_options():
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
    parser.add_argument('--weight', '-w', dest='weight', type=str,
                       help="""Insert the weight to optimize.""",
                       required=False, default="distance")
    parser.add_argument('--points', '-ps', dest='points', type=str,
                       help="""Insert the osm identifier of the points to visit (the first is the origin and destination point)""",
                       required=False, default='random')
    parser.add_argument('--num_points', '-nps', dest='num_points', type=int,
                       help="""Insert the number of points to visit (only if points = random)""",
                       required=False, default=5)
    parser.add_argument('--mapName', '-mn', dest='map_filename', type=str,
                       help="""Insert the name of the file containing the map with the computed path.""",
                       required=False, default='tsp_map.html')
    return parser


def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    neo4jconn = Neo4jConnection(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    neo4jconn.open_connection()

    tsp = TSP()
    
    if(options.points == 'random'):
        sa = SelectAmenities()
        amenities = sa.select_amenity(neo4jconn)
        amenities = sa.amenity_to_df(amenities)
        points = np.random.choice(amenities['rj_osm_id'].values, options.num_points)
    else:
        points = options.points.split()
        points = [str(p) for p in points]
        if(len(point)>6):
            print("Please select a lower number of points (max 6)")
            return 0
        
 
    with neo4jconn.driver.session() as session:
        query = """call gds.graph.project(
        'subgraph_tsp', 
        ['FootNode'], 
        ['ROUTE'], 
        {nodeProperties: ['lat', 'lon'], relationshipProperties: ['%s']})"""%(str(options.weight))
        session.run(query)
    
    best_path = tsp.find_best_path(neo4jconn, points, options.weight)
    
    ordered_footnodes = best_path[0]
    cost = best_path[1]
    path = best_path[2]
    
    print("Ordered FootNode to visit:" + str(ordered_footnodes))
    print("Total cost:" + str(cost))
    # print("Path:" + str(path))
    
    
    
    # coordinates = greeter.get_coordinates(final_path = str(final_path))
    # print(coordinates)
    # m = fo.Map(location=[coordinates[0][0][0][0], coordinates[0][0][0][1]], zoom_start=13)
    # if len(coordinates[0][0]) == 0:
    #         print('\nNo result for query')
    # else:
    #     fo.PolyLine(coordinates[0][0], color="green", weight=5).add_to(m)
    #     m.save(file + '.html')
    

    path_utils = PathUtils()
    
    coordinates = path_utils.get_coordinates(neo4jconn, path)
    
    m = fo.Map(location=[coordinates[0][0][0][0], coordinates[0][0][0][1]], zoom_start=13)
    if len(coordinates[0][0]) == 0:
            print('No path')
    else:
        fo.PolyLine(coordinates[0][0], color="green", weight=5).add_to(m)
        m.save(options.map_filename)

    
    distance, green_area_weight = path_utils.evaluate_path_metrics(neo4jconn, path)
    print("Distance: " + str(distance))
    print("Green area weight: " + str(green_area_weight))


    with neo4jconn.driver.session() as session:
        query = """CALL gds.graph.drop('subgraph_tsp')"""
        session.run(query)

    neo4jconn.close_connection()
    
    
    
    return 0


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))

