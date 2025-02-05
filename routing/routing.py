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

    def routing_old_style(self,source,target):
        """evaluate the best route between the source and the target
        """
        with self.driver.session() as session:
            result = session.write_transaction(self._routing_old_style,source,target) #execute_transaction
            return result
    @staticmethod
    def _routing_old_style(tx,source,target):
        tx.run("""call gds.graph.project('subgraph_routing', ['FootNode'], 
                ['FOOT_ROUTE'], 
                {nodeProperties: ['lat', 'lon'], relationshipProperties: ['comfort_cost']});
            """)
        query = """
        match (s:FootNode {id: '%s'})
        match (t:FootNode {id: '%s'})
        CALL gds.shortestPath.dijkstra.stream('subgraph_routing', {
                                            sourceNode: s,
                                            targetNode: t,
                                            relationshipWeightProperty: 'comfort_cost'
                                            })
                                            YIELD index, sourceNode, targetNode, totalCost, nodeIds, path
        with  [nodeId IN nodeIds | gds.util.asNode(nodeId).id] AS nodes_path, totalCost as weight,path as p
        unwind relationships(p) as n with startNode(n).id as start_node,endNode(n).id as end_node,nodes_path,weight
        match (fn:FootNode{id:start_node})-[r:FOOT_ROUTE]->(fn2:FootNode{id:end_node})
        return nodes_path,weight, sum(r.danger) as total_danger, sum(r.distance) as total_distance"""%(source,target)
        result = tx.run(query)
        tx.run("""call gds.graph.drop('subgraph_routing')""")
        return result.values()[0]

def routing_old_way(greeter,source,target,boolMap=False,file=''):
    start_time = time.time()
    result = greeter.routing_old_style(source,target)
    path = result[0]
    cost = result[1]
    final_path = []

    for p in path:
        if final_path:
            if str(p) != final_path[-1:][0]:
                final_path.append(str(p))
        else:
            final_path.append(str(p))
    dic= {}
    dic['exec_time']=time.time() - start_time
    dic['hops']=len(final_path)
    if (boolMap):
        #visualization of the path
        coordinates = greeter.get_coordinates(final_path = str(final_path))
        m = fo.Map(location=[coordinates[0][0][0][0], coordinates[0][0][0][1]], zoom_start=13)
        if len(coordinates[0][0]) == 0:
                print('\nNo result for query')
        else:
            fo.PolyLine(coordinates[0][0], color="green", weight=5).add_to(m)
            m.save(file + '.html')
    #evaluation of the path
    pairs = []
    for i in range(0,len(final_path)-1):
        pairs.append([final_path[i],final_path[i+1]])
    ev = greeter.evaluate_path_metrics(pairs = str(pairs))
    dic['source'] = source
    dic['target'] = target
    dic['cost'] = ev[0][0]
    dic['danger']= ev[0][1]
    dic['distance']= ev[0][2]
    #dic['#crossings']= greeter.count_crossings(pairs = str(pairs))[0][0]
    #dic['#communities']= greeter.count_communities(pairs = str(pairs))[0][0]
    greeter.drop_all_projections()
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
    parser.add_argument('--beta', '-b', dest='beta', type=float,
                        help="""Insert the beta parameter between 0 and 1. The value represent the importance of travel time on the final cost.""",
                        required=False, default = 0.5)
    parser.add_argument('--destination', '-d', dest='dest', type=str,
                       help="""Insert the osm identifier of your destination""",
                       required=False)
    parser.add_argument('--source', '-s', dest='source', type=str,
                        help="""Insert the osm identifier of your source""",
                       required=False)
    parser.add_argument('--mapName', '-mn', dest='mapName', type=str,
                       help="""Insert the name of the file containing the map with the computed path.""",
                      required=True,)
    
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
    if(options.beta > 1 or options.beta < 0):
        print("The beta parameter value is not valid, 0.5 will be used")
        options.beta = 0.5
    #5567795278 
    #1314391413   277291137 
    result = routing_old_way(greeter,options.source,options.dest,True,options.mapName)
    print("execution time:" + str(result['exec_time']))
    print("number of hops:" + str(result['hops']))
    print("total cost:" + str(result['cost']))
    print("average danger:" + str(result['danger']))
    print("total distance:" + str(result['distance']))
    
    return 0


if __name__ == "__main__":
    main()

