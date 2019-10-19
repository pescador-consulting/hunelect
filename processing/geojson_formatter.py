from storage import db_scripts
import json


def Run():

    records = db_scripts.getdistrict_list_polygon(79)

    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [[x["lng"], x["lat"]] for x in json.loads(record[1]['polygon']['paths'])]
                    ]
                },
                "properties": {
                    "district_id": record[0]
                }
            } for record in records if record[1]['polygon']['paths'] != '']
    }

    output = open('C:/Users/Tamás/Google Drive/PAGEO/KUTATÁS/GERRYMANDERING/torzs/output.json', 'w')
    json.dump(geojson, output)
    # EPSG 4326 or WGS84
    return geojson
