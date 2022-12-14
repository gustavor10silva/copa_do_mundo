# importando bibliotecas
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd


# definindo as urls de requisicao
conmebol = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2006_-_Am%C3%A9rica_do_Sul'
concacaf = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2006_-_Am%C3%A9rica_do_Norte,_Central_e_Caribe'
uefa = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2006_-_Europa'
caf = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2006_-_%C3%81frica'
afc = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2006_-_%C3%81sia'
ofc = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2006_-_Oceania'


# dataframe eliminatorias conmebol
req_conmebol = requests.get(conmebol)
bs_conmebol = bs(req_conmebol.text, features="lxml")
table_conmebol = bs_conmebol.find_all('table', attrs={'class':'wikitable', 'style':'text-align: center;'})[0]
df_conmebol_full = pd.read_html(str(table_conmebol))[0]
df_conmebol = df_conmebol_full.loc[:,'Pos.':'Pts']
df_conmebol.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/2006/df_conmebol.csv', sep=';', index=False)


# dataframe eliminatorias concacaf
req_concacaf = requests.get(concacaf)
bs_concacaf = bs(req_concacaf.text, features="lxml")
table_concacaf = bs_concacaf.find_all('table', attrs={'border':'1', 'cellpadding':'5', 'cellspacing':'0'})[3]
df_concacaf_full = pd.read_html(str(table_concacaf))[0]
df_concacaf = df_concacaf_full.loc[:,'P':'Pts']
df_concacaf.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/2006/df_concacaf.csv', sep=';', index=False)


# dataframe eliminatorias uefa
req_uefa = requests.get(uefa)
bs_uefa = bs(req_uefa.text, features="lxml")
tables_uefa = bs_uefa.find_all('table', attrs={'border':'1', 'cellpadding':'5', 'cellspacing':'0'})

df_uefa = pd.DataFrame(columns=['P', 'País', 'J', 'V', 'E', 'D', 'GP', 'GC', 'Pts'])
for i in range(8):
    df_uefa_full = pd.read_html(str(tables_uefa[i]))[0]
    df_uefa_partial = df_uefa_full.loc[:, 'P':'Pts']
    df_uefa = pd.concat([df_uefa, df_uefa_partial])
df_uefa.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/2006/df_uefa.csv', sep=';', index=False)


# dataframe eliminatorias caf
req_caf = requests.get(caf)
bs_caf = bs(req_caf.text, features="lxml")
tables_caf = bs_caf.find_all('table', attrs={'border':'1', 'cellpadding':'5', 'cellspacing':'0'})

df_caf = pd.DataFrame(columns=['P', 'País', 'J', 'V', 'E', 'D', 'GP', 'GC', 'Pts'])
for i in range(5):
    df_caf_full = pd.read_html(str(tables_caf[i]))[0]
    df_caf_partial = df_caf_full.loc[:, 'P':'Pts']
    df_caf = pd.concat([df_caf, df_caf_partial])
df_caf.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/2006/df_caf.csv', sep=';', index=False)


# dataframe eliminatorias afc
req_afc = requests.get(afc)
bs_afc = bs(req_afc.text, features="lxml")
tables_afc = bs_afc.find_all('table', attrs={'border':'1', 'cellpadding':'5', 'cellspacing':'0'})

df_afc = pd.DataFrame(columns=['P', 'País', 'J', 'V', 'E', 'D', 'GP', 'GC', 'Pts'])
for i in range(2):
    df_afc_full = pd.read_html(str(tables_afc[i]))[0]
    df_afc_partial = df_afc_full.loc[:, 'P':'Pts']
    df_afc_partial.rename(columns={'Grupo A':'País', 'Grupo B':'País'}, inplace=True)
    df_afc = pd.concat([df_afc, df_afc_partial])
df_afc.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/2006/df_afc.csv', sep=';', index=False)


# dataframe eliminatorias ofc
req_ofc = requests.get(ofc)
bs_ofc = bs(req_ofc.text, features="lxml")
table_ofc = bs_ofc.find_all('table')[5]
df_ofc_full = pd.read_html(str(table_ofc), header=0)[0]
df_ofc = df_ofc_full.loc[:,'País':'Pts']
df_ofc.insert(0, 'Pos', range(1, df_ofc.shape[0] + 1))
df_ofc.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/2006/df_ofc.csv', sep=';', index=False)