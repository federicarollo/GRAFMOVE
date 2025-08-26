import sys
import json
import time
import os
from osgeo import gdal
import numpy as np
import argparse
from tqdm import tqdm
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scipy.ndimage import map_coordinates
from utils.db_utils import Neo4jConnection


class AirQuality:

    def sample_with_window(self, raster, x_vals, y_vals, buffer_size):
        """
        Extract values from a raster using a window around the segment
        """
        half_w = buffer_size // 2

        sampled_values = []

        for x, y in zip(x_vals, y_vals):

            # Generate the grid around the point
            x_grid, y_grid = np.meshgrid(
                np.arange(x - half_w, x + half_w + 1),
                np.arange(y - half_w, y + half_w + 1)
            )

            # Extract the values from the raster using a bilinear interpolation (order=1)
            values = map_coordinates(raster, [y_grid.ravel(), x_grid.ravel()], order=1)

            # Mean value of the window around a single segment point
            sampled_values.append(np.mean(values))
        return np.array(sampled_values)


    def world_to_pixel(self, transform, lon, lat):
        """
        Convert world coordinates to pixel coordinates
        """
        x_origin, pixel_width, _, y_origin, _, pixel_height = transform

        pixel_x = (lon - x_origin) / pixel_width
        pixel_y = (lat - y_origin) / pixel_height

        return pixel_x, pixel_y


    def sample_raster_along_line(self, raster_path, coordinate_pair, buffer_size):
        """
        Sample a raster along a segment defined by two points in the world coordinates
        """
        raster = gdal.Open(raster_path)
        if raster is None:
            print("Error: raster not found")
            return

        band = raster.GetRasterBand(1)
        transform = raster.GetGeoTransform()

        data = band.ReadAsArray(0, 0, raster.RasterXSize, raster.RasterYSize)

        px0, py0 = self.world_to_pixel(transform, coordinate_pair[0][0], coordinate_pair[0][1])
        px1, py1 = self.world_to_pixel(transform, coordinate_pair[1][0], coordinate_pair[1][1])

        # Generate the x and y values for the segment
        x_vals = np.linspace(px0, px1)
        y_vals = np.linspace(py0, py1)

        air_qualities = self.sample_with_window(data, x_vals, y_vals, buffer_size)

        mean_value = np.mean(air_qualities)

        return mean_value
        

    def add_edge_air_quality(self, conn, id_pairs, all_day_airquality, pollutant):
        with conn.driver.session() as session:
            query = """
            UNWIND $pairs AS pair
            MATCH (s:RouteNode)-[r:ROUTE]->(d:RouteNode)
            WHERE r."""+pollutant+""" is null and s.id = pair.source AND d.id = pair.destination
            SET r."""+pollutant+""" = pair.mean_air_quality, 
            r."""+pollutant+"""_per_meter = [x in pair.mean_air_quality | x*r.distance]
            WITH s, d, pair
            MATCH (d)-[r2:ROUTE]->(s)
            SET r2."""+pollutant+""" = pair.mean_air_quality, 
            r2."""+pollutant+"""_per_meter = [x in pair.mean_air_quality | x*r2.distance]
            RETURN count(r2)*2
            """
            print(query)
            result = session.run(query, pairs=[{'source': pair[0], 'destination': pair[1], 'mean_air_quality': all_day_aq}
                                          for pair, all_day_aq in zip(id_pairs, all_day_airquality)])
            summary = result.consume()
            
            return summary.counters.properties_set

    
    def set_combined_weight(self, conn, pollutant, pollutant_ratio=0.7, green_area_ratio=0.3):
        with conn.driver.session() as session:
            query = """
            CALL {
                MATCH (s:RouteNode)-[r:ROUTE]->(d:RouteNode)
                RETURN 
                    max(r."""+pollutant+"""_per_meter) AS max_pollutant_per_meter, 
                    max(r.green_area_weight) AS max_inv_ga,
                    min(r."""+pollutant+"""_per_meter) AS min_pollutant_per_meter,
                    min(r.green_area_weight) AS min_inv_ga
            }
            
            MATCH (s:RouteNode)-[r:ROUTE]->(d:RouteNode)
            WHERE r."""+pollutant+"""_green_area is null
            WITH 
                r, s, d, max_pollutant_per_meter, max_inv_ga, min_pollutant_per_meter, min_inv_ga,
                (r."""+pollutant+"""_per_meter - min_pollutant_per_meter) / (max_pollutant_per_meter - min_pollutant_per_meter) AS normalized_pollutant,
                (r.green_area_weight - min_inv_ga) / (max_inv_ga - min_inv_ga) AS normalized_inv_ga
                            
            WITH 
                r, s, d,
               ("""+str(pollutant_ratio)+""" * normalized_pollutant) + ("""+str(green_area_ratio)+""" * normalized_inv_ga) AS weighted_average
            
            SET r."""+pollutant+"""_green_area = weighted_average
            
            WITH r, s, d, weighted_average
            MATCH (d)-[r2:ROUTE]->(s)
            SET r2."""+pollutant+"""_green_area = weighted_average
            
            RETURN count(r)
            """

            result = session.run(query)
            summary = result.consume()
            
            return summary.counters.properties_set




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
    parser.add_argument('--interpolation_filenames', '-i', dest='interpolation_filenames', type=str,
                        help="""Insert the list of names of the files with the interpolated air quality data.""",
                        required=True)
    parser.add_argument('--buffer_size', '-b', dest='buffer_size', type=int,
                        help="""Insert the size of the buffer (in meters) to compute average air quality data.""",
                        required=True)
    parser.add_argument('--pollutant_name', '-pn', dest='pollutant_name', type=str,
                        help="""Insert the name of the pollutant (this will be the name of the new property in the graph).""",
                        required=True)
    return parser


def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    neo4jconn = Neo4jConnection(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    neo4jconn.open_connection()
    path = neo4jconn.get_path()[0][0] + '/' + neo4jconn.get_import_folder_name()[0][0] + '/'

    aq = AirQuality()

    gdal.UseExceptions()
    
    interpolation_filename_list = (options.interpolation_filenames).split(',')
    print(interpolation_filename_list)

    raster_files = [path+filename.strip() for filename in interpolation_filename_list]
    print(raster_files)

    edges = neo4jconn.get_edges_endpoints()
    id_pairs = []
    
    mean_air_quality_values_all = []


    print(f"Start sampling raster along {len(edges)} edges (this operation may take a while)...")
    start_time = time.time()
    for raster_file in raster_files:
        mean_air_quality_values = []
        for edge in tqdm(range(len(edges)), desc='Sample raster along line...'):
        # for edge in edges:
            # print(edge)
            source_id, destination_id, source_lon, source_lat, destination_lon, destination_lat = edges[edge]
            # print(source_id)
            # print(destination_id)
            
            # Find the mean air quality along the segment
            mean_air_quality = aq.sample_raster_along_line(raster_file, [(source_lon, source_lat), (destination_lon, destination_lat)], options.buffer_size)
        
            id_pairs.append([source_id, destination_id])
            mean_air_quality_values.append(mean_air_quality)
        mean_air_quality_values_all.append(mean_air_quality_values)
    
    print("Time to sample raster: ", time.time() - start_time)
    
    all_day_airquality = [list(element) for element in zip(*mean_air_quality_values_all)]
    
    # for pair, all_day_aq in zip(id_pairs, all_day_airquality):
    #     print(pair[0])
    #     print(pair[1])
    #     print(all_day_aq)
        
        
    result = aq.add_edge_air_quality(neo4jconn, id_pairs, all_day_airquality, options.pollutant_name)
    print("Set " + str(result) + " pollutant properties")

    # result = aq.set_combined_weight(neo4jconn, options.pollutant_name)
    # print("Set " + str(result) + " combined weight properties")

    neo4jconn.close_connection()



if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Execution time: %s seconds ---" % (time.time() - start_time))
