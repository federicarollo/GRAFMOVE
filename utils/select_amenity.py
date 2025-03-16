import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import overpy
import json
from neo4j import GraphDatabase
import argparse
import pandas as pd
from utils.db_utils import Neo4jConnection


class SelectAmenities:

    def select_amenity(self, conn):
        """Select amenities with the desired tags."""
        with conn.driver.session() as session:
            
            query = """
            MATCH (f:FootNode)<-[r:NEAR]-(n:OSMNode:POI)-[:TAGS]-(t:Tag) 
            where n.name is not null and 
            (t.tourism='attraction' or t.amenity='place_of_worship' or t.place='square' or t.amenity='fountain')
            with n.name as poi_name, collect(f.id) as footnodes, collect(r.distance) as distances
            with poi_name, footnodes, distances, apoc.coll.indexOf(distances,min(distances)) as min_index 
            with poi_name, footnodes[min_index] as footnode, distances[min_index] as dist
            match (rj:FootNode {id: footnode}), (poi:POI {name: poi_name}) 
            return collect(rj.id) as osm_id, collect([rj.lat, rj.lon]) as gps_coordinates, collect(poi.name) as name
            """
            result = session.run(query)
            nodes = result.values()[0]

            query = """
            MATCH (f:FootNode)<-[r:NEAR]-(:OSMNode)-[:PART_OF]->(n:OSMWay:POI)-[:TAGS]-(t:Tag) 
            where n.name is not null and 
            (t.tourism='attraction' or t.amenity='place_of_worship' or t.place='square' or t.amenity='fountain')
            with n.name as poi_name, collect(f.id) as footnodes, collect(r.distance) as distances
            with poi_name, footnodes, distances, apoc.coll.indexOf(distances,min(distances)) as min_index 
            with poi_name, footnodes[min_index] as footnode, distances[min_index] as dist
            match (rj:FootNode {id: footnode}), (poi:OSMWay:POI {name: poi_name}) 
            return collect(rj.id) as osm_id, collect([rj.lat, rj.lon]) as gps_coordinates, collect(poi.name) as name
            """
            result = session.run(query)
            ways = result.values()[0]
            
            
            amenities = [nodes[0] + ways[0], nodes[1] + ways[1], nodes[2] + ways[2]]
            
            return amenities
        
    def amenity_to_df(self, amenities):
        
        amenities_dict = {'rj_osm_id': [], 'lat': [], 'lon': [], 'poi_name': []}
        
        for osm_id, gps_coordinates, name in zip(amenities[0], amenities[1], amenities[2]):
            amenities_dict['rj_osm_id'].append(osm_id)
            amenities_dict['lat'].append(gps_coordinates[0])
            amenities_dict['lon'].append(gps_coordinates[1])
            amenities_dict['poi_name'].append(name)

        amenities_df = pd.DataFrame(amenities_dict)
        amenities_df.to_csv('amenities.csv', index=False)
        
        return amenities_df


    def select_amenity_in_bbox(self, amenities, lat_min, lat_max, lon_min, lon_max):
        
        amenities_in_bbox = {'rj_osm_id': [], 'lat': [], 'lon': [], 'poi_name': []}
        
        for osm_id, gps_coordinates, name in zip(amenities[0], amenities[1], amenities[2]):
            
            if(gps_coordinates[0] > lat_min and gps_coordinates[0] < lat_max and gps_coordinates[1] > lon_min and gps_coordinates[1] < lon_max):
                
                amenities_in_bbox['rj_osm_id'].append(osm_id)
                amenities_in_bbox['lat'].append(gps_coordinates[0])
                amenities_in_bbox['lon'].append(gps_coordinates[1])
                amenities_in_bbox['poi_name'].append(name)

        amenities_in_bbox_df = pd.DataFrame(amenities_in_bbox)
        amenities_in_bbox_df.to_csv('amenities_in_bbox.csv', index=False)
        
        return amenities_in_bbox_df
    
    
    
    

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
    parser.add_argument('--bbox', '-b', dest='bbox', type=bool,
                        help="""Select only amenities in the bbox.""",
                        required=False, default=False)
    parser.add_argument('--latitude_min', '-latmin', dest='latmin', type=float,
                       help="""The minimum latitude of the bounding box.""",
                       required=False)
    parser.add_argument('--latitude_max', '-latmax', dest='latmax', type=float,
                       help="""The maximum latitude of the bounding box.""",
                       required=False)
    parser.add_argument('--longitude_min', '-lonmin', dest='lonmin', type=float,
                       help="""The minimum longitude of the bounding box.""",
                       required=False)
    parser.add_argument('--longitude_max', '-lonmax', dest='lonmax', type=float,
                       help="""The maximum longitude of the bounding box.""",
                       required=False)
    return parser


def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    neo4jconn = Neo4jConnection(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    neo4jconn.open_connection()
    path = neo4jconn.get_path()[0][0] + '\\' + neo4jconn.get_import_folder_name()[0][0] + '\\'
    
    sa = SelectAmenities()

    amenities = sa.select_amenity(neo4jconn)
    amenities_df = sa.amenity_to_df(amenities)
    
    
    
    if (options.bbox):        
        lat_min = options.latmin
        lat_max = options.latmax
        lon_min = options.lonmin
        lon_max = options.lonmax
        amenities_in_city_center_df = sa.select_amenity_in_bbox(amenities, lat_min, lat_max, lon_min, lon_max)
        

    neo4jconn.close_connection()

    return 0


if __name__ == "__main__":
    main()