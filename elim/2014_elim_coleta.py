# importando bibliotecas
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd


# definindo as urls de requisicao
conmebol = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2014_-_Am%C3%A9rica_do_Sul'
concacaf = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2014_-_Am%C3%A9rica_do_Norte,_Central_e_Caribe'
uefa = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2014_-_Europa'
caf = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2014_-_%C3%81frica'
afc = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2014_-_%C3%81sia'
#ofc = 'https://pt.wikipedia.org/wiki/Eliminat%C3%B3rias_da_Copa_do_Mundo_FIFA_de_2014_-_Oceania'
# nenhuma selecao da ofc se classificou, portanto nao coletaremos os dados dessas selecoes


# dataframe eliminatorias conmebol
req_conmebol = requests.get(conmebol)
bs_conmebol = bs(req_conmebol.text, features="lxml")
table_conmebol = bs_conmebol.find_all('table', attrs={'class':'wikitable', 'style':'text-align: center;'})[0]
df_conmebol_full = pd.read_html(str(table_conmebol))[0]
df_conmebol = df_conmebol_full.loc[:,'Pos.':'SG']
df_conmebol.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/2014/df_conmebol.csv', sep=';', index=False)


# dataframe eliminatorias concacaf
req_concacaf = requests.get(concacaf)
bs_concacaf = bs(req_concacaf.text, features="lxml")
table_concacaf = bs_concacaf.find_all('table', attrs={'class':'wikitable', 'style':'text-align: center;'})[9]
df_concacaf_full = pd.read_html(str(table_concacaf))[0]
df_concacaf = df_concacaf_full.loc[:,'Seleção':'SG']
df_concacaf.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/2014/df_concacaf.csv', sep=';', index=False)


# dataframe eliminatorias uefa
req_uefa = requests.get(uefa)
bs_uefa = bs(req_uefa.text, features="lxml")
tables_uefa = bs_uefa.find_all('table', attrs={'class':'wikitable', 'style':'text-align: center;'})

df_uefa = pd.DataFrame(columns=['Pos.', 'vdeSeleção', 'Pts', 'J', 'V', 'E', 'D', 'GP', 'GC', 'SG'])
for i in range(9):
    df_uefa_full = pd.read_html(str(tables_uefa[i]))[0]
    df_uefa_partial = df_uefa_full.loc[:, 'Pos.':'SG']
    df_uefa = pd.concat([df_uefa, df_uefa_partial])
df_uefa.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/2014/df_uefa.csv', sep=';', index=False)


# dataframe eliminatorias caf
req_caf = requests.get(caf)
bs_caf = bs(req_caf.text, features="lxml")
tables_caf = bs_caf.find_all('table', attrs={'class':'wikitable', 'style':'text-align: center;'})

df_caf = pd.DataFrame(columns=['Pos', 'Seleção', 'Pts', 'J', 'V', 'E', 'D', 'GP', 'GC', 'SG'])
for i in range(10):
    df_caf_full = pd.read_html(str(tables_caf[i]))[0]
    df_caf_partial = df_caf_full.loc[:, 'Seleção':'SG']
    df_caf_partial.insert(0, 'Pos', [1, 2, 3, 4])
    df_caf = pd.concat([df_caf, df_caf_partial])
df_caf.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/2014/df_caf.csv', sep=';', index=False)


# dataframe eliminatorias afc
req_afc = requests.get(afc)
bs_afc = bs(req_afc.text, features="lxml")
tables_afc = bs_afc.find_all('table', attrs={'class':'wikitable', 'style':'text-align: center;'})

df_afc = pd.DataFrame(columns=['Pos', 'Seleção', 'Pts', 'J', 'V', 'E', 'D', 'GP', 'GC', 'SG'])
for i in range(2):
    df_afc_full = pd.read_html(str(tables_afc[i]))[0]
    df_afc_partial = df_afc_full.loc[:, 'Seleção':'SG']
    df_afc_partial.insert(0, 'Pos', [1, 2, 3, 4, 5])
    df_afc = pd.concat([df_afc, df_afc_partial])
df_afc.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/2014/df_afc.csv', sep=';', index=False)