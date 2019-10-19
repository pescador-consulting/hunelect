import psycopg2
import json

with open('./config/config.json') as json_file:
    config = json.load(json_file)

db_password = config["db_password"]
db_host = config["db_host"]
db_db = config["db_db"]
db_user = config["db_user"]


def insert_national(district_id, election_id, pname, results, orderrank):
    sql = """INSERT INTO gerry.national_results(district_id, election_id, pname, results, orderrank)
            VALUES ( %s, %s, %s, %s, %s);"""
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(district_id), str(election_id), pname, results, orderrank,))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return


def batch_insert_national(district_id, election_id, df):
    df = df.T
    i = 1
    for index, row in df.iterrows():
        insert_national(district_id, election_id, row[0], row[1], i)
        i += 1
    return


def insert_personal(district_id, election_id, hname, pname, results):
    sql = """INSERT INTO public.r_personal(district_id, election_id, hname, pname, results)
            VALUES ( %s, %s, %s, %s, %s);"""
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(district_id), str(election_id), hname, pname, results,))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return


def batch_insert_personal(district_id, election_id, df):
    df = df.T
    for index, row in df.iterrows():
        insert_personal(district_id, election_id, row[0], row[1], row[2])
    return


def insert_description(district_id, election_id, sname, stype):
    sql = """INSERT INTO gerry.street(district_id, election_id, sname, stype)
            VALUES ( %s, %s, %s, %s);"""
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(district_id), str(election_id), sname, stype,))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return


def batch_insert_description(district_id, election_id, df):
    df = df.T
    for index, row in df.iterrows():
        insert_description(district_id, election_id, row[0], row[1])
    return


def insert_participation(district_id, election_id, results):
    sql = """INSERT INTO gerry.participation(district_id, election_id, nszvsz, mj, nsz, ulbnszsz, ulbszsz, easzmsz, elszsz, eszsz, vszsz, sznlvsz, atvsz, knszvsz, szmvsz, akszbbsz, avnmigjszvsz, aszm, asznsz)
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);;"""
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(district_id), str(election_id), str(results.get('nszvsz', 0)), str(results.get('mj', 0)), str(results.get('nsz', 0)), str(results.get('ulbnszsz', 0)), str(results.get('ulbszsz', 0)), str(results.get('easzmsz', 0)), str(results.get('elszsz', 0)), str(results.get('eszsz', 0)), str(results.get('vszsz', 0)), str(results.get('sznlvsz', 0)), str(results.get('atvsz', 0)), str(results.get('knszvsz', 0)), str(results.get('szmvsz', 0)), str(results.get('akszbbsz', 0)), str(results.get('avnmigjszvsz', 0)), str(results.get('aszm', 0)), str(results.get('asznsz', 0)),))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return


def batch_insert_participation(district_id, election_id, results):
    insert_participation(district_id, election_id, results)
    return


def create_election(e_year, e_type):
    sql = """INSERT INTO gerry.election(e_year, e_type)
            VALUES ( %s, %s) RETURNING id;"""
    conn = None
    election_id = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(e_year), str(e_type),))
        election_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return election_id


def create_mhref(election_id, maz, evk, list_url):
    sql = """INSERT INTO gerry.mhref(election_id, maz, evk, list_url)
            VALUES ( %s, %s, %s, %s) RETURNING id;"""
    conn = None
    mhref_id = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(election_id), str(maz), str(evk), str(list_url),))
        mhref_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return mhref_id


def delete_election(e_year, e_type):
    sql = """DELETE FROM gerry.election
            WHERE e_year=%s and e_type=%s;"""
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(e_year), str(e_type),))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return


def getdistrictid(maz, taz, sorszam):
    sql = """SELECT * FROM gerry.district d
            WHERE d.maz=%s and d.taz=%s and d.sorsz=%s;"""
    conn = None
    records = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(maz), str(taz), str(sorszam),))
        records = cur.fetchall()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return records


def getdistrict_list_polygon(election_id):
    sql = """SELECT id,polygon
    FROM gerry.district
    where election_id = %s
    and polygon is not null;"""
    conn = None
    records = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(election_id),))
        records = cur.fetchall()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return records


def getemptydistrictid(election_id):
    sql = """SELECT gerry.getemptydistrictid(%s);"""
    conn = None
    records = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(election_id),))
        records = cur.fetchall()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return records


def inserthrefs(national_election, local_election, participation, details, shape, mhref_id, district_id):
    sql = """INSERT INTO gerry.href(national_election, local_election, participation, details, shape, mhref_id, district_id)
            VALUES ( %s, %s, %s, %s, %s, %s, %s) ;"""
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (national_election, local_election, participation, details, shape, str(mhref_id), str(district_id),))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return


def updatedistrictshape(district_id, shape):
    sql = """UPDATE gerry.district SET polygon = %s WHERE id = %s;"""
    conn = None
    if type(shape) == str:
        shape = json.loads(shape)
    else:
        shape = json.loads(shape.decode('utf8').replace("'", '"'))
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (json.dumps(shape), str(district_id),))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return


def insert_district(district, election_id):
    sql = """INSERT INTO gerry.district(election_id, maz, taz, sorsz, evk, tip, cimt, cimk, href)
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s);;"""
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(election_id), str(district.get('maz', 0)), str(district.get('taz', 0)), str(district.get('sorsz', 0)), str(district.get('evk', 0)), str(district.get('tip', 0)), str(district.get('cimt', 0)), str(district.get('cimk', 0)), str(district.get('href', 0)),))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return


def uploadbatchofdistrict(districts, election_id):
    for district in districts:
        insert_district(district, election_id)
    return


def insert_mayor(district_id, election_id, hname, pname, results, orderrank):
    sql = """INSERT INTO gerry.mayor_results(district_id, election_id, hname, pname, result, orderank)
            VALUES ( %s, %s, %s, %s, %s, %s);"""
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(district_id), str(election_id), hname, pname, str(results), str(orderrank),))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return


def batch_insert_mayor(district_id, election_id, df):
    df = df.T
    i = 1
    for index, row in df.iterrows():
        insert_mayor(district_id, election_id, row[0], row[1], row[2], i)
        i += 1
    return


def insert_evk(district_id, election_id, hname, pname, results, orderrank):
    sql = """INSERT INTO gerry.evk_results(district_id, election_id, hname, pname, result, orderank)
            VALUES ( %s, %s, %s, %s, %s, %s);"""
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(district_id), str(election_id), hname, pname, str(results), str(orderrank),))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return


def batch_insert_evk(district_id, election_id, df):
    df = df.T
    i = 1
    for index, row in df.iterrows():
        insert_evk(district_id, election_id, row[0], row[1], row[2], i)
        i += 1
    return


def insert_capital(district_id, election_id, hname, pname, results, orderrank):
    sql = """INSERT INTO gerry.capital_results(district_id, election_id, hname, pname, result, orderank)
            VALUES ( %s, %s, %s, %s, %s, %s);"""
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(district_id), str(election_id), hname, pname, str(results), str(orderrank),))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return


def batch_insert_capital(district_id, election_id, df):
    df = df.T
    i = 1
    for index, row in df.iterrows():
        insert_capital(district_id, election_id, row[0], row[1], row[2], i)
        i += 1
    return


def insert_county(district_id, election_id, pname, results, orderrank):
    sql = """INSERT INTO gerry.county_results(district_id, election_id, pname, results, orderrank)
            VALUES ( %s, %s, %s, %s, %s);"""
    conn = None
    try:
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.execute(sql, (str(district_id), str(election_id), pname, str(results), str(orderrank),))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return


def batch_insert_county(district_id, election_id, df):
    df = df.T
    i = 1
    for index, row in df.iterrows():
        insert_county(district_id, election_id, row[0], row[1], i)
        i += 1
    return
