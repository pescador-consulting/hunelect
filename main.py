import time
from bs4 import BeautifulSoup

from processing import webfunctions
from subroutes import subtables
from storage import db_scripts

db_scripts.clean_db()

for i in range(1, 2):
    for j in range(1, 20):
        county = str(i)
        oevk = str(j)
        # url = 'https://www.valasztas.hu/szavazokori-eredmenyek?_szavazokorok_WAR_nvinvrportlet_formDate=32503680000000&p_p_id=szavazokorok_WAR_nvinvrportlet&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&p_p_col_id=column-2&p_p_col_pos=1&p_p_col_count=2&_szavazokorok_WAR_nvinvrportlet_vlId=244&_szavazokorok_WAR_nvinvrportlet_vltId=556&_szavazokorok_WAR_nvinvrportlet_megyeKod=&_szavazokorok_WAR_nvinvrportlet_telepulesKod=&_szavazokorok_WAR_nvinvrportlet_searchSortColumn=SORSZAM&_szavazokorok_WAR_nvinvrportlet_searchSortType=asc&_szavazokorok_WAR_nvinvrportlet_wardClean=false&_szavazokorok_WAR_nvinvrportlet_wardSettlement=false&_szavazokorok_WAR_nvinvrportlet_megyeKod2=' + county + '&_szavazokorok_WAR_nvinvrportlet_telepulesKod2=&_szavazokorok_WAR_nvinvrportlet_valasztasTipusKod=&_szavazokorok_WAR_nvinvrportlet_evkSzam=&_szavazokorok_WAR_nvinvrportlet_szavkorTypes=&_szavazokorok_WAR_nvinvrportlet_valasztasIntegralt=false&_szavazokorok_WAR_nvinvrportlet_oevkKod=' + oevk + '&_szavazokorok_WAR_nvinvrportlet_searchText=Baranya+megye+1.+sz%C3%A1m%C3%BA+OEVK&_szavazokorok_WAR_nvinvrportlet_searchWard='
        url = 'https://www.valasztas.hu/szavazokori-eredmenyek?_szavazokorok_WAR_nvinvrportlet_formDate=32503680000000&p_p_id=szavazokorok_WAR_nvinvrportlet&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&p_p_col_id=column-2&p_p_col_pos=1&p_p_col_count=2&_szavazokorok_WAR_nvinvrportlet_vlId=244&_szavazokorok_WAR_nvinvrportlet_vltId=556&_szavazokorok_WAR_nvinvrportlet_megyeKod=&_szavazokorok_WAR_nvinvrportlet_telepulesKod=&_szavazokorok_WAR_nvinvrportlet_searchSortColumn=&_szavazokorok_WAR_nvinvrportlet_searchSortType=asc&_szavazokorok_WAR_nvinvrportlet_wardClean=false&_szavazokorok_WAR_nvinvrportlet_wardSettlement=false&_szavazokorok_WAR_nvinvrportlet_megyeKod2=01&_szavazokorok_WAR_nvinvrportlet_telepulesKod2=&_szavazokorok_WAR_nvinvrportlet_valasztasTipusKod=&_szavazokorok_WAR_nvinvrportlet_evkSzam=&_szavazokorok_WAR_nvinvrportlet_szavkorTypes=&_szavazokorok_WAR_nvinvrportlet_valasztasIntegralt=false&_szavazokorok_WAR_nvinvrportlet_oevkKod=' + oevk + '&_szavazokorok_WAR_nvinvrportlet_searchText=Budapest+1.+sz%C3%A1m%C3%BA+OEVK'
        time.sleep(1)
        raw_html = webfunctions.simple_get(url)
        html = BeautifulSoup(raw_html, 'html.parser')
        oevk = html.findAll("div", {"class": "span6"})
        oevk = [x for x in oevk if 'számú OEVK' in x.text]

        print(i)
        print(j)

        if len(oevk) > 0:
            href_list, name, oevk = subtables.GetLinksOfRegion(url)
            for idx in range(0, len(href_list)):
                # some logic about creating a record
                district_id = db_scripts.insert_district(i, j, name[idx], oevk[idx], idx)
                href = href_list[idx]
                df, results, national_link, detailed_link = subtables.GetPersonalTable(href)
                db_scripts.batch_insert_participation(district_id, results)
                db_scripts.batch_insert_personal(district_id, df)
                df = subtables.GetDescriptionTable(detailed_link)
                db_scripts.batch_insert_description(district_id, df)
                df = subtables.GetNationalTable(national_link)
                db_scripts.batch_insert_national(district_id, df)
                time.sleep(1)
                print(idx)

time.sleep(1)

for i in range(2, 20):
    for j in range(1, 20):
        if i < 10:
            county = '0' + str(i)
        else:
            county = str(i)
        oevk = str(j)
        url = 'https://www.valasztas.hu/szavazokori-eredmenyek?_szavazokorok_WAR_nvinvrportlet_formDate=32503680000000&p_p_id=szavazokorok_WAR_nvinvrportlet&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&p_p_col_id=column-2&p_p_col_pos=1&p_p_col_count=2&_szavazokorok_WAR_nvinvrportlet_vlId=244&_szavazokorok_WAR_nvinvrportlet_vltId=556&_szavazokorok_WAR_nvinvrportlet_megyeKod=&_szavazokorok_WAR_nvinvrportlet_telepulesKod=&_szavazokorok_WAR_nvinvrportlet_searchSortColumn=SORSZAM&_szavazokorok_WAR_nvinvrportlet_searchSortType=asc&_szavazokorok_WAR_nvinvrportlet_wardClean=false&_szavazokorok_WAR_nvinvrportlet_wardSettlement=false&_szavazokorok_WAR_nvinvrportlet_megyeKod2=' + county + '&_szavazokorok_WAR_nvinvrportlet_telepulesKod2=&_szavazokorok_WAR_nvinvrportlet_valasztasTipusKod=&_szavazokorok_WAR_nvinvrportlet_evkSzam=&_szavazokorok_WAR_nvinvrportlet_szavkorTypes=&_szavazokorok_WAR_nvinvrportlet_valasztasIntegralt=false&_szavazokorok_WAR_nvinvrportlet_oevkKod=' + oevk + '&_szavazokorok_WAR_nvinvrportlet_searchText=Baranya+megye+1.+sz%C3%A1m%C3%BA+OEVK&_szavazokorok_WAR_nvinvrportlet_searchWard='
        time.sleep(1)
        raw_html = webfunctions.simple_get(url)
        html = BeautifulSoup(raw_html, 'html.parser')
        oevk = html.findAll("div", {"class": "span6"})
        oevk = [x for x in oevk if 'számú OEVK' in x.text]

        print(i)
        print(j)

        if len(oevk) > 0:
            href_list, name, oevk = subtables.GetLinksOfRegion(url)
            for idx in range(0, len(href_list)):
                # some logic about creating a record
                district_id = db_scripts.insert_district(i, j, name[idx], oevk[idx], idx)
                href = href_list[idx]
                df, results, national_link, detailed_link = subtables.GetPersonalTable(href)
                db_scripts.batch_insert_participation(district_id, results)
                db_scripts.batch_insert_personal(district_id, df)
                df = subtables.GetDescriptionTable(detailed_link)
                db_scripts.batch_insert_description(district_id, df)
                df = subtables.GetNationalTable(national_link)
                db_scripts.batch_insert_national(district_id, df)
                time.sleep(1)
                print(idx)

time.sleep(1)