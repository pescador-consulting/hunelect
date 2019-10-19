import pandas as pd


def LoadDistrictCSV():
    df = pd.read_csv('C:/sqlite3/szavazokorok.csv', sep=',', header=0)
    df.values
    return df.values
