import sys
import os
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from ast import operator
from neo4j import GraphDatabase


class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = None

    def open_connection(self):
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close_connection(self):
        if self.driver:
            self.driver.close()

    def drop_projection(self, projection_name):
        with self.driver.session() as session:
            query = f"""
            CALL gds.graph.list() YIELD graphName as name
            WHERE name = '{projection_name}'
            RETURN name
            """
            result = session.run(query)
            if result.single():
                drop_query = f"CALL gds.graph.drop('{projection_name}')"
                session.run(drop_query)

    def drop_all_projections(self):
        with self.driver.session() as session:
            query = f"""
            CALL gds.graph.list() YIELD graphName
            CALL gds.graph.drop(graphName)
            YIELD database
            RETURN 'dropped ' + graphName
            """
            session.run(query)

    def get_coordinates(self,nodes):
        with self.driver.session() as session:
            query = f"""
            unwind '{nodes}' as node 
            match (n:RouteNode{id: node}) return collect([n.lat,n.lon])"""%(nodes)
            result = session.run(query)
            return result.values()

    def get_edges_endpoints(self):
        with self.driver.session() as session:
            query = """
            MATCH (s:RouteNode)-[r:ROUTE]->(d:RouteNode)
            RETURN s.id AS source, d.id AS destination, 
            s.lon AS source_lon, s.lat AS source_lat, 
            d.lon AS destination_lon, d.lat AS destination_lat
            """
            result = session.run(query)
            return result.values()

    def get_extreme_lon_lat(self):
        with self.driver.session() as session:
            query = """
            MATCH (n:RouteNode)
            RETURN min(n.lon) as min_lon, max(n.lon) as max_lon, min(n.lat) as min_lat, max(n.lat) as max_lat
            """
            result = session.run(query)
            return result.values()[0]

    def get_path(self):
        with self.driver.session() as session:
            query = """
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.neo4j_home' return value;
                    """
            result = session.run(query)
            return result.values()

    def get_import_folder_name(self):
        with self.driver.session() as session:
            query = """
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.import' return value;
                    """
            result = session.run(query)
            return result.values()


    def generate_spatial_layer(self, name):
        """generate the spatial layer of the project"""
        with self.driver.session() as session:
            result = session.run(f"""
                CALL spatial.addPointLayer('{name}');
                """)
                # call spatial.addWKTLayer('spatial_node', 'geometry')
            return result.values()
        
       