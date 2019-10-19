from adm_2019.subroutes import subtables
from adm_2019 import districts as district_list
from storage import db_scripts


def Run(election_id = None, mhref_id = None):
    if election_id is None:
        districts_list = district_list.GetDistricts()
        db_scripts.delete_election(2019, 'adm')
        election_id = db_scripts.create_election(2019, 'adm')
        for element in districts_list:
            subtables.getdistrictsofcounty(element[0], element[1], element[2], election_id)
    if mhref_id is None:
        mhref_id = db_scripts.create_mhref(election_id, 0, 0, '')

    district = db_scripts.getemptydistrictid(election_id)
    while district:
        results = None
        national_election = None
        local_election = None
        participation = None
        details = None
        shape = None
        district = district[0][0][1:-1].split(',')
        mayor, evk, county, capital, participation, details, shape = subtables.create_hrefs(district)
        db_scripts.inserthrefs(county, mayor, participation, details, shape, mhref_id, district[0])
        results = subtables.process_district(mayor, evk, county, capital, participation, details, shape)
        subtables.uploadresults(district, election_id, results)
        # try:
        #     national_election, local_election, participation, details, shape = subtables.create_hrefs(list(district[0]))
        #     db_scripts.inserthrefs(national_election, local_election, participation, details, shape, mhref_id, district[0][0])
        #     results = subtables.process_district(national_election, details, shape)
        #     subtables.uploadresults(list(district[0]), election_id, results)
        # except Exception as e:
        #     print('error with district id: %d', district[0][0])
        #     print(str(e))
        district = db_scripts.getemptydistrictid(election_id)
    return
