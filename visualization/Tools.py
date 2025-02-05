import pandas as pd
import geopandas as gpd

"""This file contains some functions useful to the data extraction process"""

def save_gdf(gdf, path, filename):
    """save the GeoPandas Dataframe in a json file"""


    gdf.to_crs(epsg=4326, inplace=True)
    df = pd.DataFrame(gdf)
    df['geometry'] = df['geometry'].astype(str)
    df.to_json(path + filename, orient='table')

def elem_to_feature(elem, geomType):
    """Convert the element in a json format"""

    if geomType == "LineString":
        prop = {}
        for key in elem['tags'].keys():
            if key in ['highway','bicycle','foot','lanes','cycleway','segregated','maxspeed','bridge','incline','lit','smoothness', 'surface','wheelchair']:
                prop[key]=elem['tags'][key]
        prop['nodes']=elem['nodes']
        return {
            "geometry": {
                    "type": geomType,
                    "coordinates": [[d["lon"], d["lat"]] for d in elem["geometry"]]
            },
            "properties": prop
        }
    
    if geomType == "Polygon":
        return [(d["lat"], d["lon"]) for d in elem["geometry"]]
    
    return {
        "geometry": {
            "type": geomType,
            "coordinates": [elem["geometry"]["coordinates"][0], elem["geometry"]["coordinates"][1]]
            #"coordinates": [elem["lon"], elem["lat"]]
        },
        "properties": {}
    }
