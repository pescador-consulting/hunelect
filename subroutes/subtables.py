import pandas as pd
from bs4 import BeautifulSoup
from processing import webfunctions
import html5lib


def GetDescriptionTable(url):
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


def GetLinksOfRegion(url):
    raw_html = webfunctions.simple_get(url)

    html = BeautifulSoup(raw_html, 'html.parser')
    table = html.findAll("a")
    table = [x for x in table if hasattr(x, 'attrs')]
    table = [x for x in table if 'href' in x.attrs]
    table = [x for x in table if 'szavkorSorszam' in x.attrs['href']]
    table = [x for x in table if 'Szavazóhelyiség címe' not in x.text]
    href_list = [x.attrs['href'] for x in table]

    name = [x.text for x in table]

    oevk = html.findAll("div", {"class": "span6"})
    oevk = [x.text for x in oevk if 'számú OEVK' in x.text]

    return href_list, name, oevk
