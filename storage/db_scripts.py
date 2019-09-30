import psycopg2
import json

with open('./config/config.json') as json_file:
    config = json.load(json_file)

db_password = config["db_password"]
db_host = config["db_host"]
db_db = config["db_db"]
db_user = config["db_user"]


def clean_db():
    conn = None
    try:
        # read database configuration
        # connect to the PostgreSQL database
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        # create a new cursor
        cur = conn.cursor()
        cur.execute("TRUNCATE participation, district, r_national, r_personal, street RESTART IDENTITY;")
        conn.commit()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return


def insert_district(county, oevk, dname, oevk_name, idx):
    """ insert a new district into the district table """
    sql = """INSERT INTO public.district(county_id, oevk, dname, oevk_name, idx)
            VALUES ( %s, %s, %s, %s, %s) RETURNING district_id;"""
    conn = None
    district_id = None
    try:
        # read database configuration
        # connect to the PostgreSQL database
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (str(county), str(oevk), dname, oevk_name, str(idx),))
        # get the generated id back
        district_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return district_id


def insert_national(district_id, pname, results):
    """ insert a new district into the district table """
    sql = """INSERT INTO public.r_national(district_id, pname, results)
            VALUES ( %s, %s, %s);"""
    conn = None
    try:
        # read database configuration
        # connect to the PostgreSQL database
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (str(district_id), pname, results,))
        # get the generated id back
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return


def batch_insert_national(district_id, df):
    df = df.T
    for index, row in df.iterrows():
        insert_national(district_id, row[0], row[1])
    return


def insert_personal(district_id, hname, pname, results):
    """ insert a new district into the district table """
    sql = """INSERT INTO public.r_personal(district_id, hname, pname, results)
            VALUES ( %s, %s, %s, %s);"""
    conn = None
    try:
        # read database configuration
        # connect to the PostgreSQL database
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (str(district_id), hname, pname, results,))
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return


def batch_insert_personal(district_id, df):
    df = df.T
    for index, row in df.iterrows():
        insert_personal(district_id, row[0], row[1], row[2])
    return


def insert_description(district_id, sname, stype):
    """ insert a new district into the district table """
    sql = """INSERT INTO public.street(district_id, sname, stype)
            VALUES ( %s, %s, %s);"""
    conn = None
    try:
        # read database configuration
        # connect to the PostgreSQL database
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (str(district_id), sname, stype,))
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return


def batch_insert_description(district_id, df):
    df = df.T
    for index, row in df.iterrows():
        insert_description(district_id, row[0], row[1])
    return


def insert_participation(district_id, nszvsz, mj, nsz, ulbnszsz, ulbszsz, easzmsz, elszsz, eszsz):
    """ insert a new district into the district table """
    sql = """INSERT INTO public.participation(district_id, nszvsz, mj, nsz, ulbnszsz, ulbszsz, easzmsz, elszsz, eszsz)
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s);;"""
    conn = None
    try:
        # read database configuration
        # connect to the PostgreSQL database
        conn = psycopg2.connect(host=db_host, database=db_db, user=db_user, password=db_password)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (str(district_id), str(nszvsz), str(mj), str(nsz), str(ulbnszsz), str(ulbszsz), str(easzmsz), str(elszsz), str(eszsz),))
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return district_id


def batch_insert_participation(district_id, results):
    insert_participation(district_id, results[0], results[1], results[2], results[3], results[4], results[5], results[6], results[7])
    return
