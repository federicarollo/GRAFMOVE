import sys
import json
import time
import os
# from osgeo import gdal
import numpy as np
import argparse
from tqdm import tqdm
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point, box
from shapely import Polygon, buffer
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from scipy.ndimage import map_coordinates
from utils.db_utils import Neo4jConnection


class CrashRisk:

    def add_edge_crash_risk(self, conn, id_pairs, crash_risk_values, area_values):
        with conn.driver.session() as session:
            query = """
            UNWIND $pairs AS pair
            MATCH (s:RouteNode)-[r:ROUTE]->(d:RouteNode)
            WHERE r.crash_risk is null and s.id = pair.source AND d.id = pair.destination
            SET r.crash_risk_per_meter = [x IN pair.crash_risk | x / pair.area * r.distance], r.crash_risk = [x IN pair.crash_risk | x], r.crash_risk_density = [x IN pair.crash_risk | x / pair.area]
            WITH s, d, pair
            MATCH (d)-[r2:ROUTE]->(s)
            SET r2.crash_risk_per_meter = [x IN pair.crash_risk | x / pair.area * r2.distance], r2.crash_risk = [x IN pair.crash_risk | x], r2.crash_risk_density = [x IN pair.crash_risk | x / pair.area]
            RETURN count(r2)*2
            """
            print(query)
            result = session.run(query, pairs=[{'source': pair[0], 'destination': pair[1], 'crash_risk': crash_risk, 'area': area}
                                          for pair, crash_risk, area in zip(id_pairs, crash_risk_values, area_values)])
            summary = result.consume()
            
            return summary.counters.properties_set

    def add_edge_crash_risk_norm(self, conn):
        with conn.driver.session() as session:
            query = """
            MATCH ()-[r:ROUTE]->() with min(r.crash_risk_density[0]) as min_density_n, max(r.crash_risk_density[0]) as max_density_n,
            min(r.crash_risk_density[1]) as min_density_m, max(r.crash_risk_density[1]) as max_density_m,
            min(r.crash_risk_density[2]) as min_density_a, max(r.crash_risk_density[2]) as max_density_a,
            min(r.crash_risk_density[3]) as min_density_e, max(r.crash_risk_density[3]) as max_density_e
            MATCH ()-[r:ROUTE]->()
            set r.crash_risk_density_norm = [(r.crash_risk_density[0]-min_density_n)/(max_density_n-min_density_n), (r.crash_risk_density[1]-min_density_m)/(max_density_m-min_density_m), (r.crash_risk_density[2]-min_density_a)/(max_density_a-min_density_a), (r.crash_risk_density[3]-min_density_e)/(max_density_e-min_density_e)]
            """
            print(query)
            result = session.run(query)
            summary = result.consume()
            
            return summary.counters.properties_set

def get_time_interval(time_str):
    time_str = time_str.zfill(4)

    hour = int(time_str[:2])
    minute = int(time_str[2:])
    total_minutes = hour * 60 + minute

    if total_minutes >= 22*60 or total_minutes < 4*60:
        return "night"
    elif total_minutes < 10*60:
        return "morning"
    elif total_minutes < 16*60:
        return "afternoon"
    else:
        return "evening"



def add_options():
    parser = argparse.ArgumentParser(description='Insertion of Air Quality data.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--accident_filename', '-a', dest='accident_filename', type=str,
                        help="""Insert the name of the file with the geolocated accidents.""",
                        required=False, default="interpolated_data.tif")
    parser.add_argument('--buffer_size', '-b', dest='buffer_size', type=int,
                        help="""Insert the size of the buffer (in meters) to compute crash risk.""",
                        required=True)
    return parser


def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    neo4jconn = Neo4jConnection(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    neo4jconn.open_connection()
    # path = neo4jconn.get_path()[0][0] + '/' + neo4jconn.get_import_folder_name()[0][0] + '/'

    cr = CrashRisk()

    edges = neo4jconn.get_edges_endpoints()
    id_pairs = []
    crash_risk_values = []
    area_values = []

    accident_file = options.accident_filename
    accidents = gpd.read_file(accident_file)
    accidents['datainc'] = pd.to_datetime(accidents['datainc'], format="%Y%m%d")
    accidents['year'] = accidents['datainc'].dt.year
    accidents["time_interval"] = accidents["orainc"].apply(get_time_interval)

    gdf_accidents = gpd.GeoDataFrame(accidents, geometry="geometry").to_crs("EPSG:4326")
    # gdf_accidents = gdf_accidents.groupby(['geometry', 'year', 'time_interval']).size().unstack().reset_index().fillna(0)

    print(gdf_accidents)
    gdf_accidents.to_csv("gdf_accidents.csv")
    
    b = options.buffer_size
    
    current_year = datetime.now().year

    print(f"Start sampling data along {len(edges)} edges (this operation may take a while)...")
    start_time = time.time()
    non_empty=0
    empty=0
    accident_index = []
    for edge in tqdm(range(len(edges)), desc='Sample raster along line...'):
        source_id, destination_id, source_lon, source_lat, destination_lon, destination_lat = edges[edge]

        point1 = Point(source_lon, source_lat)
        point1_utm = gpd.GeoDataFrame(geometry=[point1], crs="EPSG:4326").to_crs("EPSG:32632")
        point1_utm = point1_utm.geometry.iloc[0]
        x1= point1_utm.x
        y1= point1_utm.y

        point2 = Point(destination_lon, destination_lat)
        point2_utm = gpd.GeoDataFrame(geometry=[point2], crs="EPSG:4326").to_crs("EPSG:32632")
        point2_utm = point2_utm.geometry.iloc[0]
        x2 = point2_utm.x
        y2 = point2_utm.y

        # bbox = box(min(x1, x2) - buffer//2, min(y1, y2) - buffer//2, max(x1, x2) + buffer//2, max(y1, y2) + buffer//2)
        distance = b//2
        bbox = buffer(LineString([(x1,y1), (x2,y2)]), distance, cap_style='square')
        area = bbox.area
        bbox_wgs84 = gpd.GeoSeries([bbox], crs="EPSG:32632").to_crs("EPSG:4326")
        # print(bbox_wgs84[0])
        # if (source_lon == 10.9688434 and source_lat == 44.6285666 and destination_lon==10.967661 and destination_lat==44.6290337):
        #     print(bbox_wgs84[0])
        
        gdf_accidents_within_bbox = gdf_accidents[gdf_accidents.geometry.within(bbox_wgs84[0])]  #, align=True)]
        gdf_accidents_within_bbox = gdf_accidents_within_bbox.groupby(['year', 'time_interval']).size().unstack().reset_index().fillna(0)
        
        
        if(gdf_accidents_within_bbox.shape[0]>0):
            non_empty+=1
            gdf_accidents_within_bbox['crash_risk_night'] = gdf_accidents_within_bbox.get('night', 0) / (1 + (current_year - gdf_accidents_within_bbox['year']))
            gdf_accidents_within_bbox['crash_risk_morning'] = gdf_accidents_within_bbox.get('morning', 0) / (1 + (current_year - gdf_accidents_within_bbox['year']))
            gdf_accidents_within_bbox['crash_risk_evening'] = gdf_accidents_within_bbox.get('evening', 0) / (1 + (current_year - gdf_accidents_within_bbox['year']))
            gdf_accidents_within_bbox['crash_risk_afternoon'] = gdf_accidents_within_bbox.get('afternoon', 0) / (1 + (current_year - gdf_accidents_within_bbox['year']))
        else:
            empty+=1
            gdf_accidents_within_bbox['crash_risk_night'] = 0
            gdf_accidents_within_bbox['crash_risk_morning'] = 0
            gdf_accidents_within_bbox['crash_risk_evening'] = 0
            gdf_accidents_within_bbox['crash_risk_afternoon'] = 0
        
        # crash_risk = {
        #     'crash_risk_night': gdf_accidents_within_bbox['crash_risk_night'].sum(),
        #     'crash_risk_morning': gdf_accidents_within_bbox['crash_risk_morning'].sum(),
        #     'crash_risk_afternoon': gdf_accidents_within_bbox['crash_risk_afternoon'].sum(),
        #     'crash_risk_evening': gdf_accidents_within_bbox['crash_risk_evening'].sum()
        # }
        
        crash_risk = [gdf_accidents_within_bbox['crash_risk_night'].sum(), gdf_accidents_within_bbox['crash_risk_morning'].sum(),
                        gdf_accidents_within_bbox['crash_risk_afternoon'].sum(), gdf_accidents_within_bbox['crash_risk_evening'].sum()]

        id_pairs.append([source_id, destination_id])
        crash_risk_values.append(crash_risk)
        area_values.append(area)
    
    print("Time to sample raster: ", time.time() - start_time)
    
    result = cr.add_edge_crash_risk(neo4jconn, id_pairs, crash_risk_values, area_values)
    print("Set " + str(result) + " crash risk properties")
    
    result = cr.add_edge_crash_risk_norm(neo4jconn)
    print("Set " + str(result) + " crash risk properties")

    print("--------------------------------")
    print("No accidents found for " + str(empty) + " edges (" + str(empty/len(edges)*100) + "%)")
    print("At least one accident found for " + str(non_empty) + " edges (" + str(non_empty/len(edges)*100) + "%)")
    
    neo4jconn.close_connection()



if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))
