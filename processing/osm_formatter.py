import osmnx as ox
import pydriosm as dri


subregion_name = 'hungary'
customised_data_dir = 'C:/Users/Tamás/Downloads/hungary-latest.osm.pbf'
customised_data_dir = 'C:/Users/Tamás/Google Drive/PAGEO/KUTATÁS/GERRYMANDERING/torzs/'


def Run():
    osmdb = dri.OSM()
    hungary = dri.read_osm_pbf(subregion_name, data_dir=customised_data_dir)

    # graph = ox.graph_from_file('C:/Users/Tamás/Downloads/hungary-latest.osm.pbf')
    osmdb.connect_db(database_name='osm_pbf_data_extracts')
    osmdb.dump_osm_pbf_data(
        hungary,
        table_name=subregion_name,
        parsed=True,
        if_exists='replace',
        chunk_size=None,
        download_confirmation_required=True,
        rm_raw_file=False,
        subregion_name_as_table_name=True)
    print(hungary)
    return
