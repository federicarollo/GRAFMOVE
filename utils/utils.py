import sys
import os

class Utils:
    
    def elem_to_feature(elem, geomType):

        if geomType == "LineString":
            prop = {}
            if("tags" in elem.keys()):
                for key in elem['tags'].keys():
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
                #"coordinates": [elem["geometry"]["coordinates"][0], elem["geometry"]["coordinates"][1]]
                "coordinates": [elem["lon"], elem["lat"]]
            },
            "properties": {}
        }