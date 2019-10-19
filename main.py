from eu_2019 import main as eu_2019
from adm_2019 import main as adm_2019
from processing import geojson_formatter, osm_formatter

eu_2019.Run(election_id = 74, mhref_id = 66)

adm_2019.Run(election_id = 79, mhref_id = 68)

# geojson_formatter.Run()
osm_formatter.Run()
