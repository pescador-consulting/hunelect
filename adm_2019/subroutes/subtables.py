import pandas as pd
import re
from bs4 import BeautifulSoup
from processing import webfunctions
import html5lib
from urllib import request
from storage import db_scripts


def padtolen(number, length):
    number = str(number)
    while len(number) < length:
        number = '0' + number
    return number


def create_hrefs(district):
    district[1] = padtolen(district[1], 2)
    district[2] = padtolen(district[2], 3)
    district[3] = str(district[3])  # https://www.
    mayor = 'https://www.valasztas.hu/szavazokorok_onk2019?p_p_id=onkszavazokorieredmenyek_WAR_nvinvrportlet&p_p_lifecycle=1&p_p_state=maximized&p_p_mode=view&_onkszavazokorieredmenyek_WAR_nvinvrportlet_tabId=tab1&_onkszavazokorieredmenyek_WAR_nvinvrportlet_telepulesKod=' + district[2] + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_tabId2=POLGARMESTER_VALASZTAS&_onkszavazokorieredmenyek_WAR_nvinvrportlet_searchSortColumn=SORSZAM&_onkszavazokorieredmenyek_WAR_nvinvrportlet_searchSortType=asc&_onkszavazokorieredmenyek_WAR_nvinvrportlet_megyeKod=' + district[1] + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vlId=294&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vltId=687&_onkszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam=' + district[3]
    evk = 'https://www.valasztas.hu/szavazokorok_onk2019?p_p_id=onkszavazokorieredmenyek_WAR_nvinvrportlet&p_p_lifecycle=1&p_p_state=maximized&p_p_mode=view&_onkszavazokorieredmenyek_WAR_nvinvrportlet_tabId=tab1&_onkszavazokorieredmenyek_WAR_nvinvrportlet_telepulesKod=' + district[2] + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_tabId2=EVK_KEPVISELO_VALASZTASA&_onkszavazokorieredmenyek_WAR_nvinvrportlet_searchSortColumn=SORSZAM&_onkszavazokorieredmenyek_WAR_nvinvrportlet_searchSortType=asc&_onkszavazokorieredmenyek_WAR_nvinvrportlet_megyeKod=' + district[1] + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vlId=294&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vltId=687&_onkszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam=' + district[3]
    county = 'https://www.valasztas.hu/szavazokorok_onk2019?p_p_id=onkszavazokorieredmenyek_WAR_nvinvrportlet&p_p_lifecycle=1&p_p_state=maximized&p_p_mode=view&_onkszavazokorieredmenyek_WAR_nvinvrportlet_tabId=tab1&_onkszavazokorieredmenyek_WAR_nvinvrportlet_telepulesKod=' + district[2] + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_tabId2=MEGYEI_KOZGYULES_VALASZTASA&_onkszavazokorieredmenyek_WAR_nvinvrportlet_searchSortColumn=SORSZAM&_onkszavazokorieredmenyek_WAR_nvinvrportlet_searchSortType=asc&_onkszavazokorieredmenyek_WAR_nvinvrportlet_megyeKod=' + district[1] + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vlId=294&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vltId=687&_onkszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam=' + district[3]
    participation = mayor
    capital = 'https://www.valasztas.hu/szavazokorok_onk2019?p_p_id=onkszavazokorieredmenyek_WAR_nvinvrportlet&p_p_lifecycle=1&p_p_state=maximized&p_p_mode=view&_onkszavazokorieredmenyek_WAR_nvinvrportlet_tabId=tab1&_onkszavazokorieredmenyek_WAR_nvinvrportlet_telepulesKod=' + district[2] + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_tabId2=FOPOLGARMESTER_VALASZTAS&_onkszavazokorieredmenyek_WAR_nvinvrportlet_searchSortColumn=SORSZAM&_onkszavazokorieredmenyek_WAR_nvinvrportlet_searchSortType=asc&_onkszavazokorieredmenyek_WAR_nvinvrportlet_megyeKod=' + district[1] + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vlId=294&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vltId=687&_onkszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam=' + district[3]
    details = 'https://www.valasztas.hu/szavazokorok_onk2019?p_p_id=onkszavazokorieredmenyek_WAR_nvinvrportlet&p_p_lifecycle=1&p_p_state=maximized&p_p_mode=view&_onkszavazokorieredmenyek_WAR_nvinvrportlet_tabId=tab2&_onkszavazokorieredmenyek_WAR_nvinvrportlet_telepulesKod=' + district[2] + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_megyeKod=' + district[1] + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vlId=294&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vltId=687&_onkszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam=' + district[3]
    shape = 'https://www.valasztas.hu/szavazokorok_onk2019?p_p_id=onkszavazokorieredmenyek_WAR_nvinvrportlet&p_p_lifecycle=2&p_p_state=maximized&p_p_mode=view&p_p_resource_id=resourceIdGetElectionMapData&p_p_cacheability=cacheLevelPage&_onkszavazokorieredmenyek_WAR_nvinvrportlet_telepulesKod=' + district[2] + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_megyeKod=' + district[1] + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vlId=294&_onkszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam=' + district[3] + '&p_p_lifecycle=1&_onkszavazokorieredmenyek_WAR_nvinvrportlet_tabId=tab2&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vltId=687'

    return mayor, evk, county, capital, participation, details, shape


def getshape(url):
    try:
        url = request.urlopen(url)
        read = url.read()
        if read == b'':
            return '{"polygon":{"paths":""}}'
    except:
        return '{"polygon":{"paths":""}}'
    return read


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


def getmayorresults(url):
    raw_html = webfunctions.simple_get(url)
    html = BeautifulSoup(raw_html, 'html.parser')
    table = html.find(lambda tag: tag.name == 'table')
    if not table:
        return None, None
    else:
        rows = table.findAll(lambda tag: tag.name == 'tr')
        l1 = []
        l2 = []
        l3 = []
        for tr in rows:
            columns = tr.find_all('span')
            l1.append(columns[0].text.strip())
            l3.append(int(re.search('(.*)\(', columns[2].text.strip()).group(0)[0:-2]))
            parties = ';'.join([span.text.strip() for span in tr.find_all('span', {'class':'_onkszavazokorieredmenyek_WAR_nvinvrportlet_popover'})])
            l2.append(parties)
        df_mayor = pd.DataFrame([l1, l2, l3])
        participation = {}
        table = html.findAll("div", {"class": "nvi-summary-content summary-content-fejadatok toggler-content-collapsed hide"})
        # if len(table) == 0:
        #     table = html.findAll("div", {"class": "nvi-summary-container summary-container-ogy first-container nvi-collapsed"})
        if len(table[0].find_all('span')) > 11:
            participation['nszvsz'] = int(table[0].find_all('span')[0].text.replace('\xa0', '').replace(' fő', ''))
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
    return df_mayor, participation


def getevkresults(url):
    raw_html = webfunctions.simple_get(url)
    html = BeautifulSoup(raw_html, 'html.parser')
    table = html.find(lambda tag: tag.name == 'table')
    if re.search('EVK-választás egyéni választókerületi szavazás eredményeinek megjelenítése', html.findAll('div', {'class': 'nvi-electoral-district-filter'})[0].text):
        if not table:
            return None
        else:
            rows = table.findAll(lambda tag: tag.name == 'tr')
            l1 = []
            l2 = []
            l3 = []
            for tr in rows:
                columns = tr.find_all('span')
                l1.append(columns[0].text.strip())
                l3.append(int(re.search('(.*)\(', columns[2].text.strip()).group(0)[0:-2]))
                parties = ';'.join([span.text.strip() for span in tr.find_all('span', {'class': '_onkszavazokorieredmenyek_WAR_nvinvrportlet_popover'})])
                l2.append(parties)
            df_evk = pd.DataFrame([l1, l2, l3])
        return df_evk
    else:
        url = url.replace('EVK_KEPVISELO_VALASZTASA', 'EGYENI_LISTAS_VALASZTAS')
        raw_html = webfunctions.simple_get(url)
        html = BeautifulSoup(raw_html, 'html.parser')
        table = html.find(lambda tag: tag.name == 'table')
        if re.search('Egyéni listás választás egyéni választókerületi szavazás eredményeinek megjelenítése', html.findAll('div', {'class': 'nvi-electoral-district-filter'})[0].text):
            if not table:
                return None
            else:
                rows = table.findAll(lambda tag: tag.name == 'tr')
                l1 = []
                l2 = []
                l3 = []
                for tr in rows:
                    columns = tr.find_all('span')
                    l1.append(columns[0].text.strip())
                    l3.append(int(re.search('(.*)\(', columns[2].text.strip()).group(0)[0:-2]))
                    parties = ';'.join([span.text.strip() for span in tr.find_all('span', {'class': '_onkszavazokorieredmenyek_WAR_nvinvrportlet_popover'})])
                    l2.append(parties)
                df_evk = pd.DataFrame([l1, l2, l3])
            return df_evk
        else:
            return None


def getcountyresults(url):
    raw_html = webfunctions.simple_get(url)
    html = BeautifulSoup(raw_html, 'html.parser')
    if re.search('Megyei közgyűlés választása egyéni választókerületi szavazás eredményeinek megjelenítése', html.findAll('div', {'class': 'nvi-electoral-district-filter'})[0].text):
        table = html.find(lambda tag: tag.name == 'table')
        l1 = []
        l2 = []
        if not table:
            return None
        else:
            rows = table.findAll(lambda tag: tag.name == 'tr')
            for tr in rows:
                columns = tr.find_all('span', {'class': 'nvi-text-normal'})
                parties = ';'.join([span.text.strip() for span in tr.find_all('span', {'class': '_onkszavazokorieredmenyek_WAR_nvinvrportlet_popover'})])
                l1.append(parties)
                l2.append(int(columns[0].text))
            df_county = pd.DataFrame([l1, l2])
        return df_county
    else:
        return None


def getcapitalresults(url):
    raw_html = webfunctions.simple_get(url)
    html = BeautifulSoup(raw_html, 'html.parser')
    if re.search('Főpolgármester-választás egyéni választókerületi szavazás eredményeinek megjelenítése', html.findAll('div', {'class': 'nvi-electoral-district-filter'})[0].text):
        table = html.find(lambda tag: tag.name == 'table')
        if not table:
            return None
        else:
            rows = table.findAll(lambda tag: tag.name == 'tr')
            l1 = []
            l2 = []
            l3 = []
            for tr in rows:
                columns = tr.find_all('span')
                l1.append(columns[0].text.strip())
                l3.append(int(re.search('(.*)\(', columns[2].text.strip()).group(0)[0:-2]))
                parties = ';'.join([span.text.strip() for span in tr.find_all('span', {'class': '_onkszavazokorieredmenyek_WAR_nvinvrportlet_popover'})])
                l2.append(parties)
            df_capital = pd.DataFrame([l1, l2, l3])
        return df_capital
    else:
        return None


def process_district(mayor, evk, county, capital, participation, details, shape):
    results = {}
    results['shape'] = getshape(shape)
    if results['shape'] == b'':
        return []
    else:
        results['details'] = getdetails(details)
        results['mayor'], results['participation'] = getmayorresults(mayor)
        results['evk'] = getevkresults(evk)
        results['county'] = getcountyresults(county)
        results['capital'] = getcapitalresults(capital)
        return results


def uploadresults(district, election_id, results):
    if results:
        if results['participation']:
            db_scripts.batch_insert_participation(district[0], election_id, results['participation'])
        if type(results['details']) is pd.core.frame.DataFrame:
            db_scripts.batch_insert_description(district[0], election_id, results['details'])
        if type(results['mayor']) is pd.core.frame.DataFrame:
            db_scripts.batch_insert_mayor(district[0], election_id, results['mayor'])
        if type(results['evk']) is pd.core.frame.DataFrame:
            db_scripts.batch_insert_evk(district[0], election_id, results['evk'])
        if type(results['capital']) is pd.core.frame.DataFrame:
            db_scripts.batch_insert_capital(district[0], election_id, results['capital'])
        if type(results['county']) is pd.core.frame.DataFrame:
            db_scripts.batch_insert_county(district[0], election_id, results['county'])
        if results['shape']:
            db_scripts.updatedistrictshape(district[0], results['shape'])
    return


def getdistrict(maz, taz, idx):
    maz = padtolen(maz, 2)
    taz = padtolen(taz, 3)
    url = 'https://www.valasztas.hu/szavazokorok_onk2019?p_p_id=onkszavazokorieredmenyek_WAR_nvinvrportlet&p_p_lifecycle=1&p_p_state=maximized&p_p_mode=view&_onkszavazokorieredmenyek_WAR_nvinvrportlet_tabId=tab2&_onkszavazokorieredmenyek_WAR_nvinvrportlet_telepulesKod=' + taz + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_megyeKod=' + maz + '&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vlId=294&_onkszavazokorieredmenyek_WAR_nvinvrportlet_vltId=687&_onkszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam=' + str(idx)
    raw_html = webfunctions.simple_get(url)
    html = BeautifulSoup(raw_html, 'html.parser')
    table = html.findAll('div', {'class': 'szavazokorieredmenyek-details-container'})
    if table:
        district = []
        attrs = {}
        district_table = table[0].findAll('span', {'class': 'text-semibold'})
        attrs['href'] = url
        attrs['maz'] = int(maz)
        attrs['taz'] = int(taz)
        attrs['sorsz'] = int(idx)
        attrs['cimk'] = district_table[0].text.strip()
        attrs['cimt'] = re.search('^\D+', district_table[1].text.strip()).group(0).strip()
        if district_table[2].text.strip() == '-':
            attrs['evk'] = 0
        else:
            attrs['evk'] = int(re.search('^\D+(.*)\.', district_table[2].text.strip()).group(1))
        attrs['tip'] = 0
        district.append(attrs)
        return district
    else:
        return None


def getdistrictsofcounty(maz, taz, sorsz, election_id):
    districts = getdistrict(maz, taz, sorsz)
    if districts:
        db_scripts.uploadbatchofdistrict(districts, election_id)
    return
