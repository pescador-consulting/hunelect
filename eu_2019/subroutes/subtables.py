import pandas as pd
import re
from bs4 import BeautifulSoup
from processing import webfunctions
import html5lib
from urllib import request
from storage import db_scripts


def GetPersonalTable(url):
    raw_html = webfunctions.simple_get(url)

    html = BeautifulSoup(raw_html, 'html.parser')

    national_link = html.findAll('button')[2].attrs['onclick'][15:-1]
    buttons = html.findAll('a')
    detailed_link = [x for x in buttons if 'A szavazókör általános adatai' in x.text][0].attrs['href']

    table = html.find(lambda tag: tag.name == 'table')
    if table is None:
        return [], [], [], []
    else:
        rows = table.findAll(lambda tag: tag.name == 'tr')

        l1 = []
        l2 = []
        l3 = []
        for tr in rows[1:]:
            if len(tr.find_all('td')) > 1:
                l1.append(tr.find_all('td')[1].find_all('div')[1].find_all('a')[0].text)
                l2.append(tr.find_all('td')[1].find_all('div')[1].find_all('div')[0].text)
                l3.append(tr.find_all('td')[2].find_all('div')[0].text)
        df = pd.DataFrame([l1, l2, l3])

        table = html.findAll("div", {"class": "nvi-summary-container summary-container-ogy first-container nvi-collapsed not-collapsable"})
        if len(table) == 0:
            table = html.findAll("div", {"class": "nvi-summary-container summary-container-ogy first-container nvi-collapsed"})
        if len(table[0].find_all('span')) > 11:
            nszvsz = table[0].find_all('span')[0].text
            if table[0].find_all('div')[14].text == ' Megjelent ':
                m = table[0].find_all('div')[15].text
            else:
                m = table[0].find_all('div')[14].text
            if table[0].find_all('div')[14].text == ' Megjelent ':
                nsz = table[0].find_all('div')[20].text
            else:
                nsz = table[0].find_all('div')[19].text
            ulbnszsz = table[0].find_all('span')[5].text
            ulbszsz = table[0].find_all('span')[7].text
            easzmsz = table[0].find_all('span')[9].text
            elszsz = table[0].find_all('span')[11].text
            eszsz = table[0].find_all('span')[13].text
        else:
            othertable = html.findAll("div", {"class": "nvi-summary-content summary-content-kijeloltJkv toggler-content-collapsed hide"})
            nszvsz = table[0].find_all('span')[0].text
            m = othertable[0].find_all('div')[13].text
            nsz = othertable[0].find_all('div')[18].text
            ulbnszsz = 0
            ulbszsz = othertable[0].find_all('div')[22].findAll('span')[2].text
            easzmsz = 0
            elszsz = othertable[0].find_all('div')[23].findAll('span')[2].text
            eszsz = othertable[0].find_all('div')[24].findAll('span')[2].text

        results = [nszvsz, m, nsz, ulbnszsz, ulbszsz, easzmsz, elszsz, eszsz]

        return df, results, national_link, detailed_link


def GetNationalTable(url):
    raw_html = webfunctions.simple_get(url)
    html = BeautifulSoup(raw_html, 'html.parser')
    table = html.findAll("div", {"class": "nvi-search-list"})
    l1 = []
    l2 = []
    for i in range(0, len(table[0].find_all('div', {"class": "span7"}))):
        l1.append(table[0].find_all('div', {"class": "span7"})[i].text)
        l2.append(table[0].find_all('div', {"class": "span6 text-right"})[i].text)
    df = pd.DataFrame([l1, l2])

    return df


def padtolen(number, length):
    number = str(number)
    while len(number) < length:
        number = '0' + number
    return number


def create_hrefs(district):
    district[1] = padtolen(district[1], 2)
    district[2] = padtolen(district[2], 3)
    district[3] = str(district[3])  # https://www.
    national_election = 'https://portal.valasztas.hu/szavazokorok_ep2019?_epszavazokorieredmenyek_WAR_nvinvrportlet_formDate=32503680000000&p_p_id=epszavazokorieredmenyek_WAR_nvinvrportlet&p_p_lifecycle=0&p_p_state=maximized&p_p_mode=view&_epszavazokorieredmenyek_WAR_nvinvrportlet_tabId=tab1&_epszavazokorieredmenyek_WAR_nvinvrportlet_cur=1&_epszavazokorieredmenyek_WAR_nvinvrportlet_vlId=291&_epszavazokorieredmenyek_WAR_nvinvrportlet_vltId=684&_epszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam=' + district[3] + '&_epszavazokorieredmenyek_WAR_nvinvrportlet_egyeniValasztokeruletiSzavazas=false&_epszavazokorieredmenyek_WAR_nvinvrportlet_cur2=1&_epszavazokorieredmenyek_WAR_nvinvrportlet_listasSzavazas=true&_epszavazokorieredmenyek_WAR_nvinvrportlet_searchSortColumn=SORSZAM&_epszavazokorieredmenyek_WAR_nvinvrportlet_searchSortType=desc&_epszavazokorieredmenyek_WAR_nvinvrportlet_searchSortColumn2=ERVENYES_SZAVAZATOK&_epszavazokorieredmenyek_WAR_nvinvrportlet_searchSortType2=desc&_epszavazokorieredmenyek_WAR_nvinvrportlet_megyeKod=' + district[1] + '&_epszavazokorieredmenyek_WAR_nvinvrportlet_telepulesKod=' + district[2] + '&_epszavazokorieredmenyek_WAR_nvinvrportlet_listasSzavazas=true&_epszavazokorieredmenyek_WAR_nvinvrportlet_egyeniValasztokeruletiSzavazas=false#_epszavazokorieredmenyek_WAR_nvinvrportlet_paginator1'
    local_election = ''
    participation = national_election
    details = 'https://portal.valasztas.hu/szavazokorok_ep2019?p_p_id=epszavazokorieredmenyek_WAR_nvinvrportlet&p_p_lifecycle=1&p_p_state=maximized&p_p_mode=view&_epszavazokorieredmenyek_WAR_nvinvrportlet_tabId=tab2&_epszavazokorieredmenyek_WAR_nvinvrportlet_telepulesKod=' + district[2] + '&_epszavazokorieredmenyek_WAR_nvinvrportlet_megyeKod=' + district[1] + '&_epszavazokorieredmenyek_WAR_nvinvrportlet_vlId=291&_epszavazokorieredmenyek_WAR_nvinvrportlet_vltId=684&_epszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam=' + district[3]
    details = 'https://portal.valasztas.hu/szavazokorok_ep2019?p_p_id=epszavazokorieredmenyek_WAR_nvinvrportlet&p_p_lifecycle=1&p_p_state=maximized&p_p_mode=view&_epszavazokorieredmenyek_WAR_nvinvrportlet_tabId=tab2&_epszavazokorieredmenyek_WAR_nvinvrportlet_telepulesKod=' + district[2] + '&_epszavazokorieredmenyek_WAR_nvinvrportlet_megyeKod=' + district[1] + '&_epszavazokorieredmenyek_WAR_nvinvrportlet_vlId=291&_epszavazokorieredmenyek_WAR_nvinvrportlet_vltId=684&_epszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam=' + district[3]
    shape = 'https://portal.valasztas.hu/szavazokorok_ep2019?p_p_id=epszavazokorieredmenyek_WAR_nvinvrportlet&p_p_lifecycle=2&p_p_state=maximized&p_p_mode=view&p_p_resource_id=resourceIdGetElectionMapData&p_p_cacheability=cacheLevelPage&_epszavazokorieredmenyek_WAR_nvinvrportlet_telepulesKod=' + district[2] + '&_epszavazokorieredmenyek_WAR_nvinvrportlet_megyeKod=' + district[1] + '&_epszavazokorieredmenyek_WAR_nvinvrportlet_vlId=291&_epszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam=' + district[3] + '&p_p_lifecycle=1&_epszavazokorieredmenyek_WAR_nvinvrportlet_tabId=tab2&_epszavazokorieredmenyek_WAR_nvinvrportlet_vltId=684&_epszavazokorieredmenyek_WAR_nvinvrportlet_prpVlId=291&_epszavazokorieredmenyek_WAR_nvinvrportlet_prpVltId=684&_epszavazokorieredmenyek_WAR_nvinvrportlet_prpNemzetisegKod='

    return national_election, local_election, participation, details, shape


def getshape(url):
    url = request.urlopen(url)
    if url.read() == b'':
        return '{"polygon":{"paths":""}}'
    return url.read()


def getdetails(url):
    raw_html = webfunctions.simple_get(url)
    html = BeautifulSoup(raw_html, 'html.parser')
    table = html.find(lambda tag: tag.name == 'table')
    if table is not None:
        rows = table.findAll(lambda tag: tag.name == 'tr')
        l1 = []
        l2 = []
        for tr in rows:
            l1.append(tr.find_all('td')[0].find_all('div')[1].text)
            l2.append(tr.find_all('td')[0].find_all('div')[2].text)
        df = pd.DataFrame([l1, l2])
    else:
        l1 = ['egész város']
        l2 = ['egész város']
        df = pd.DataFrame([l1, l2])
    return df


def getnationalresults(url):
    raw_html = webfunctions.simple_get(url)
    html = BeautifulSoup(raw_html, 'html.parser')
    table = html.findAll("div", {"class": "nvi-search-list"})
    l1 = []
    l2 = []
    if not table:
        return None, None
    else:
        for i in range(0, len(table[0].find_all('div', {"class": "span4 text-right"}))):
            l2.append(int(table[0].find_all('div', {"class": "span4 text-right"})[i].text[:table[0].find_all('div', {"class": "span4 text-right"})[i].text.find('(')].replace('\xa0', '').replace(' ', '')))
        for i in range(0, len(table[0].find_all('div', {"class": "span8"}))):
            if table[0].find_all('div', {"class": "span8"})[i].text != ' Érvényes szavazatok (%): ':
                l1.append(table[0].find_all('div', {"class": "span8"})[i].text.strip())
        df_national = pd.DataFrame([l1, l2])
        participation = {}
        table = html.findAll("div", {"class": "nvi-summary-content summary-content-fejadatok toggler-content-collapsed hide"})
        # if len(table) == 0:
        #     table = html.findAll("div", {"class": "nvi-summary-container summary-container-ogy first-container nvi-collapsed"})
        if len(table[0].find_all('span')) > 11:
            participation['nszvsz'] = int(table[0].find_all('span')[0].text.replace('\xa0', '').replace(' fő. ', ''))
            if table[0].find_all('div')[14].text == ' Megjelent ':
                participation['mj'] = int(table[0].find_all('div')[17].text[:table[0].find_all('div')[17].text.find('fő')].replace('Megjelent', '').replace('\xa0', '').replace(' ', ''))
            else:
                participation['mj'] = int(table[0].find_all('div')[16].text[:table[0].find_all('div')[16].text.find('fő')].replace('Megjelent', '').replace('\xa0', '').replace(' ', ''))
            if table[0].find_all('div')[14].text == ' Megjelent ':
                participation['nsz'] = int(table[0].find_all('div')[20].text[:table[0].find_all('div')[20].text.find('fő')].replace('Nem szavazott', '').replace('\xa0', '').replace(' ', ''))
            else:
                participation['nsz'] = int(table[0].find_all('div')[19].text[:table[0].find_all('div')[19].text.find('fő')].replace('Nem szavazott', '').replace('\xa0', '').replace(' ', ''))
            participation['ulbnszsz'] = int(table[0].find_all('span')[5].text.replace('\xa0', ''))
            participation['ulbszsz'] = int(table[0].find_all('span')[7].text.replace('\xa0', ''))
            participation['easzmsz'] = int(table[0].find_all('span')[9].text.replace('\xa0', ''))
            participation['elszsz'] = int(table[0].find_all('span')[11].text.replace('\xa0', ''))
            participation['eszsz'] = int(table[0].find_all('span')[13].text.replace('\xa0', ''))
        else:
            othertable = html.findAll("div", {"class": "nvi-summary-content summary-content-kijeloltJkv toggler-content-collapsed hide"})
            nszvsz = int(table[0].find_all('span')[0].text.replace('\xa0', '').replace(' fő. ', ''))
            m = int(othertable[0].find_all('div')[13].text)
            nsz = int(othertable[0].find_all('div')[18].text)
            ulbnszsz = 0
            ulbszsz = int(othertable[0].find_all('div')[22].findAll('span')[2].text)
            easzmsz = 0
            elszsz = int(othertable[0].find_all('div')[23].findAll('span')[2].text)
            eszsz = int(othertable[0].find_all('div')[24].findAll('span')[2].text)
        return df_national, participation


def process_district(national_election, details, shape):
    results = {}
    results['shape'] = getshape(shape)
    if results['shape'] == b'':
        return []
    else:
        results['details'] = getdetails(details)
        results['national'], results['participation'] = getnationalresults(national_election)
        return results


def uploadresults(district, election_id, results):
    if results:
        db_scripts.batch_insert_participation(district[0], election_id, results['participation'])
        db_scripts.batch_insert_description(district[0], election_id, results['details'])
        db_scripts.batch_insert_national(district[0], election_id, results['national'])
        db_scripts.updatedistrictshape(district[0], results['shape'])
    return


def getbatchdistrict(maz, taz, idx):
    maz = padtolen(maz, 2)
    taz = padtolen(taz, 3)
    url = 'https://portal.valasztas.hu/szavazokorok_ep2019?p_p_lifecycle=0&p_p_state=maximized&p_p_mode=view&p_p_id=epszavazokorok_WAR_nvinvrportlet&_epszavazokorok_WAR_nvinvrportlet_searchSortColumn=SORSZAM&_epszavazokorok_WAR_nvinvrportlet_searchText=Debrecen+%28Hajd%C3%BA-Bihar%29&_epszavazokorok_WAR_nvinvrportlet_telepulesKod=null&_epszavazokorok_WAR_nvinvrportlet_megyeKod2=' + maz + '&_epszavazokorok_WAR_nvinvrportlet_megyeKod=null&_epszavazokorok_WAR_nvinvrportlet_vlId=291&_epszavazokorok_WAR_nvinvrportlet_searchSortType=asc&_epszavazokorok_WAR_nvinvrportlet_vltId=684&_epszavazokorok_WAR_nvinvrportlet_telepulesKod2=' + taz + '&_epszavazokorok_WAR_nvinvrportlet_valasztasTipusKod=E&_epszavazokorok_WAR_nvinvrportlet_delta=200&_epszavazokorok_WAR_nvinvrportlet_keywords=&_epszavazokorok_WAR_nvinvrportlet_advancedSearch=false&_epszavazokorok_WAR_nvinvrportlet_andOperator=true&_epszavazokorok_WAR_nvinvrportlet_resetCur=false&_epszavazokorok_WAR_nvinvrportlet_cur=' + str(idx)
    raw_html = webfunctions.simple_get(url)
    html = BeautifulSoup(raw_html, 'html.parser')
    table = html.find(lambda tag: tag.name == 'table')
    if table:
        city = html.findAll('h2')[2].text.strip()
        rows = table.findAll(lambda tag: tag.name == 'tr')
        district = []
        for row in rows:
            attrs = {}
            columns = row.findAll(lambda tag: tag.name == 'td')
            if columns[0].find_all('a'):
                atext = columns[0].find_all('a')[0].text
                atext = atext.replace('Szavazóhelyiség címe:', '')
                atext = atext.strip()
                atext = atext.split(' ', 1)[0]
                href = columns[0].find_all('a')[0].attrs['href']
                attrs['href'] = href
                attrs['maz'] = int(maz)
                attrs['taz'] = int(re.search('WAR_nvinvrportlet_telepulesKod=(.+?)&_epszavazokorieredmenyek_WAR_nvinvrportlet', href).group(1))
                attrs['sorsz'] = int(re.search('_epszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam=(.*)', href).group(1))
                attrs['cimk'] = columns[0].find_all('a')[0].find_all('div')[0].text
                if len(columns[0].find_all('a')[0].find_all('div')) > 1:
                    attrs['cimk'] = attrs['cimk'] + columns[0].find_all('a')[0].find_all('div')[1].text
                attrs['cimt'] = city
                attrs['evk'] = 0
                attrs['tip'] = 0
                if district:
                    if attrs['maz'] != district[-1]['maz'] or attrs['taz'] != district[-1]['taz'] or attrs['sorsz'] != district[-1]['sorsz']:
                        district.append(attrs)
                else:
                    district.append(attrs)
        return district
    else:
        return None


def getdistrictsofcounty(maz, taz, election_id):
    idx = 1
    districts = getbatchdistrict(maz, taz, idx)
    while districts:
        db_scripts.uploadbatchofdistrict(districts, election_id)
        idx += 1
        districts = getbatchdistrict(maz, taz, idx)
    return
