# importando bibliotecas
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd


# definindo as urls de requisicao
conmebol = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2022_%E2%80%93_CONMEBOL'
concacaf = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2022_%E2%80%93_CONCACAF'
uefa = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2022_%E2%80%93_UEFA'
caf = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2022_%E2%80%93_CAF'
afc = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2022_%E2%80%93_AFC'
# ofc = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2022_%E2%80%93_OFC'
# nenhuma selecao da ofc se classificou, portanto nao coletaremos os dados dessas selecoes


# dataframe eliminatorias conmebol
req_conmebol = requests.get(conmebol)
bs_conmebol = bs(req_conmebol.text, features="lxml")
table_conmebol = bs_conmebol.find_all('table', attrs={'class':'wikitable', 'style':'text-align:center;'})[0]
df_conmebol_full = pd.read_html(str(table_conmebol))[0]
df_conmebol = df_conmebol_full.loc[:,'Pos':'Classificação']
df_conmebol.to_csv('bronze/2022/df_conmebol.csv', sep=';', index=False)


# dataframe eliminatorias concacaf
req_concacaf = requests.get(concacaf)
bs_concacaf = bs(req_concacaf.text, features="lxml")
table_concacaf = bs_concacaf.find_all('table', attrs={'class':'wikitable', 'style':'text-align: center;'})[6]
df_concacaf_full = pd.read_html(str(table_concacaf))[0]
df_concacaf = df_concacaf_full.loc[:,'Pos.':'SG']
df_concacaf['Classificação'] = ['Copa do Mundo de 2022']*3 + ['Repescagem intercontinental'] + ['Eliminados']*4
df_concacaf.to_csv('bronze/2022/df_concacaf.csv', sep=';', index=False)


# dataframe eliminatorias uefa
req_uefa = requests.get(uefa)
bs_uefa = bs(req_uefa.text, features="lxml")
tables_uefa = bs_uefa.find_all('table', attrs={'class':'wikitable', 'style':'text-align:center;'})

df_uefa = pd.DataFrame(columns=['Pos', 'Equipe', 'Pts', 'J', 'V', 'E', 'D', 'GP', 'GC', 'SG', 'Classificação'])
for i in range(10):
    df_uefa_full = pd.read_html(str(tables_uefa[i]))[0]
    df_uefa_partial = df_uefa_full.loc[:, 'Pos':'Classificação']
    df_uefa = pd.concat([df_uefa, df_uefa_partial])
df_uefa.to_csv('bronze/2022/df_uefa.csv', sep=';', index=False)


# dataframe eliminatorias caf
req_caf = requests.get(caf)
bs_caf = bs(req_caf.text, features="lxml")
tables_caf = bs_caf.find_all('table', attrs={'class':'wikitable', 'style':'text-align:center;'})

df_caf = pd.DataFrame(columns=['Pos.', 'vdeSeleção', 'Pts', 'J', 'V', 'E', 'D', 'GP', 'GC', 'SG'])
for i in range(10):
    df_caf_full = pd.read_html(str(tables_caf[i]))[0]
    df_caf_partial = df_caf_full.loc[:, 'Pos.':'SG']
    df_caf = pd.concat([df_caf, df_caf_partial])
df_caf.to_csv('bronze/2022/df_caf.csv', sep=';', index=False)


# dataframe eliminatorias afc
req_afc = requests.get(afc)
bs_afc = bs(req_afc.text, features="lxml")
tables_afc = bs_afc.find_all('table', attrs={'class':'wikitable', 'style':'text-align:center;'})

df_afc = pd.DataFrame(columns=['Pos', 'Equipe', 'Pts', 'J', 'V', 'E', 'D', 'GP', 'GC', 'SG', 'Classificação'])
for i in range(2):
    df_afc_full = pd.read_html(str(tables_afc[i]))[0]
    df_afc_partial = df_afc_full.loc[:, 'Pos':'Classificação']
    df_afc = pd.concat([df_afc, df_afc_partial])
df_afc.to_csv('bronze/2022/df_afc.csv', sep=';', index=False)


