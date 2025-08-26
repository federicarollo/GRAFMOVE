import pandas as pd
import sys
import os
import json
from osgeo import gdal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_utils import Neo4jConnection
import xml.etree.ElementTree as ET
import time
import argparse

class Interpolation:
                                
                                
    def interpolate(self, conn, csv_file, pollutant_name, raster_path, power=4, radius1=300, radius2=300):
        """
        Interpolate the sensor measures in a raster file
        """
        
        csv_data = pd.read_csv(csv_file)
        pollutant_data = csv_data[pollutant_name]
        
        print("Pollutant: ")
        print(pollutant_data)
        print("Power: " + str(power))
        print("R1: " + str(radius1))
        print("R2: " + str(radius2))
        
        # Find min and max of latitude and longitude of the sensor nodes
        sensor_lon_min, sensor_lon_max = csv_data["longitude"].min(), csv_data["longitude"].max()
        sensor_lat_min, sensor_lat_max = csv_data["latitude"].min(), csv_data["latitude"].max()

        # Find min and max of latitude and longitude of the RoadJunction node of graph
        road_lon_min, road_lon_max, road_lat_min, road_lat_max = conn.get_extreme_lon_lat()

        x_max = max(sensor_lon_max, road_lon_max)
        x_min = min(sensor_lon_min, road_lon_min)
        y_max = max(sensor_lat_max, road_lat_max)
        y_min = min(sensor_lat_min, road_lat_min)

        buffer_percent = 0.05  # Extend the bounds by 5%
        x_buffer = (x_max - x_min) * buffer_percent
        y_buffer = (y_max - y_min) * buffer_percent

        x_min -= x_buffer
        x_max += x_buffer
        y_min -= y_buffer
        y_max += y_buffer


        gdal.Grid(raster_path, csv_file[:-4]+'.vrt',
                  algorithm=f"invdist:power={power}:radius1={radius1}:radius2={radius2}:smoothing=0.02",
                  outputBounds=[x_min, y_min, x_max, y_max])

        return raster_path


    def write_vrt(self, csv_filename, vrt_filename, pollutant_name, x_field='longitude', y_field='latitude', layer_name='sensors', srs='EPSG:4326'):
        
        root = ET.Element('OGRVRTDataSource')

        layer = ET.SubElement(root, 'OGRVRTLayer', name=csv_filename[:-4].split('\\')[-1].split('/')[-1])
        ET.SubElement(layer, 'SrcDataSource').text = csv_filename
        ET.SubElement(layer, 'GeometryType').text = 'wkbPoint'
        ET.SubElement(layer, 'LayerSRS').text = srs

        geometry = ET.SubElement(layer, 'GeometryField', encoding='PointFromColumns', x=x_field, y=y_field, z=pollutant_name)

        tree = ET.ElementTree(root)
        tree.write(vrt_filename, encoding='UTF-8', xml_declaration=True)



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
    parser.add_argument('--interpolation_filename', '-i', dest='interpolation_filename', type=str,
                        help="""Insert the name of the file to store the interpolated air quality data.""",
                        required=False, default="interpolated_data.tif")
    parser.add_argument('--csv_file', '-csv', dest='csv_file', type=str,
                        help="""Insert the name of the file with the data to interpolate.""",
                        required=True)
    parser.add_argument('--pollutant_name', '-pn', dest='pollutant_name', type=str,
                        help="""Insert the name of the pollutant (this will be the name of the new property in the graph).""",
                        required=True)
    parser.add_argument('--idw_power', '-pw', dest='idw_power', type=int,
                        help="""IDW power""",
                        required=False, default=4)
    parser.add_argument('--idw_radius1', '-r1', dest='idw_radius1', type=int,
                        help="""IDW radius 1""",
                        required=False, default=300)
    parser.add_argument('--idw_radius2', '-r2', dest='idw_radius2', type=int,
                        help="""IDW radius 2""",
                        required=False, default=300)

    return parser


def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    neo4jconn = Neo4jConnection(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    neo4jconn.open_connection()
    path = neo4jconn.get_path()[0][0] + '/' + neo4jconn.get_import_folder_name()[0][0] + '/'
    
    gdal.UseExceptions()
    
    interpolation = Interpolation()
    
    csv_file = str(options.csv_file)
    interpolation.write_vrt(path+csv_file, path+csv_file[:-4]+'.vrt', options.pollutant_name)


    raster_path = interpolation.interpolate(neo4jconn, path+csv_file, options.pollutant_name, 
                                path+options.interpolation_filename,
                                options.idw_power, options.idw_radius1,
                                options.idw_radius2)

    print(f"Created raster file {raster_path}")

    return raster_path


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))
