from eu_2019.subroutes import subtables
from storage import db_scripts


def Run(election_id = None, mhref_id = None):
    if election_id is None:
        db_scripts.delete_election(2019, 'eu')
        election_id = db_scripts.create_election(2019, 'eu')
        for maz in range(1, 21):
            for taz in range(1, 400):
                subtables.getdistrictsofcounty(maz, taz, election_id)
        # districts = local_files.LoadDistrictCSV()
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
        # district = db_scripts.getdistrictid(districts[i][0], districts[i][1], districts[i][2])
        district = district[0][0][1:-1].split(',')
        national_election, local_election, participation, details, shape = subtables.create_hrefs(district)
        db_scripts.inserthrefs(national_election, local_election, participation, details, shape, mhref_id, district[0])
        results = subtables.process_district(national_election, details, shape)
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
