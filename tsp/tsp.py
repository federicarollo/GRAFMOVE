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

    def evaluate_path_metrics(self,pairs):
        """evaluate path metrics
        """
        with self.driver.session() as session:
            result = session.write_transaction(self._evaluate_path_metrics,pairs)
            return result
    @staticmethod
    def _evaluate_path_metrics(tx,pairs):
        query = """unwind %s as pairs
                match (n:FootNode{id: pairs[0]})-[r:FOOT_ROUTE]->(m:FootNode{id:pairs[1]})
                with min(r.cost) as min_cost, pairs
                match (n:FootNode{id: pairs[0]})-[r:FOOT_ROUTE]->(m:FootNode{id:pairs[1]})
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
        match (n:FootNode{id: p}) return collect([n.lat,n.lon])"""%(final_path)
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

    def routing_tsp(self,points):
        """evaluate the best route between the source and the target
        """
        with self.driver.session() as session:
            result = session.write_transaction(self._routing_tsp,points) #execute_transaction
            return result
    @staticmethod
    def _routing_tsp(tx,points):
        tx.run("""call gds.graph.create('subgraph_routing', ['FootNode'], 
                ['FOOT_ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: ['distance']});
            """)
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
				  'FOOT_ROUTE',
				  'distance', 'lat', 'lon') 
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
						return orderedFootnodesIds, totalCost, footnodeRouteIds+lastpath as totalPath"""%(points)

        print(query)

        result = tx.run(query)
        #print(list(result))
        #for r in result.values()[0]:
        #    print("STAMPO")
        #    print(r)
        tx.run("""call gds.graph.drop('subgraph_routing')""")
        return result.values()[0]

def find_best_path(greeter,points,boolMap=False,file=''):
    start_time = time.time()
    result = greeter.routing_tsp(points)
    #print(result)

    dic = {}
    dic["path"] = result[0]
    dic["cost"] = result[1]
    final_path = result[2]
    print(dic)

    dic['exec_time']=time.time() - start_time
    dic['hops']=len(final_path)
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
			
    #greeter.drop_all_projections()
    return dic

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
    parser.add_argument('--points', '-ps', dest='points', type=list,
                       help="""Insert the osm identifier of the points to visit (the first is the origin and destination point)""",
                       required=False, default = ['10911594251', '10911594425', '10911594443', '10917017001', '10911594239', '10911587741'])
# "6341815000","2021402106","1787128164","5303704141","5304947702","2021511952","315580126","1352341299","1256958142","4712448240","1256958133"
# "10917017350", "10911594286", "10917017015", "10917017269", "10917017335"
# "10681444158", "11065366611", "1627367784", "7619003474", "5905023674", "5477225907", "1344463148", "359743769", "2199223548", "4611974715" --> retrieved on neo4j through amenity
    parser.add_argument('--mapName', '-mn', dest='mapName', type=str,
                       help="""Insert the name of the file containing the map with the computed path.""",
                       required=True)
    
    #parser.add_argument('--mode', '-m', dest='mode', type=str,
    #                    help="""Choose the modality of routing : cycleways, footways, community or old.""",
    #                    required=True)
   
    #parser.add_argument('--latitude', '-x', dest='lat', type=float,
    #                    help="""Insert latitude of your starting location""",
    #                    required=False)
    #parser.add_argument('--longitude', '-y', dest='lon', type=float,
    #                    help="""Insert longitude of your starting location""",
     #                   required=False)
    #parser.add_argument('--latitude_dest', '-x_dest', dest='lat_dest', type=float,
      #                  help="""Insert latitude of your destination location""",
     #                   required=False)
    #parser.add_argument('--longitude_dest', '-y_dest', dest='lon_dest', type=float,
      #                  help="""Insert longitude of your destination location""",
     #                   required=False)
    #parser.add_argument('--alg', '-a', dest='alg', type=str,
     #                   help="""Choose the modality of routing : astar (a) or dijkstra (d).""",
     #                   required=False, default = 'd')
    #parser.add_argument('--weight', '-w', dest='weight', type=str,help="""Insert the weight to use in order to perform the routing : travel_time, cost or both.""",
    #                    required=False, default = 'both')
    
    
   
    return parser


def main(args=None):
    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    #path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'
    result = find_best_path(greeter,options.points,True,options.mapName)
    print("execution time:" + str(result['exec_time']))
    print("number of hops:" + str(result['hops']))
    print("total cost:" + str(result['cost']))
    print("ordered path:" + str(result['path']))
    
    return 0


if __name__ == "__main__":
    main()

