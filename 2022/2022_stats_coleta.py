#%%
# importando bibliotecas
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from unidecode import unidecode


df_classif_2022 = pd.read_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/gold/2022/df_classif_2022.csv', sep=';')
selecoes_classificadas = list(df_classif_2022['selecao'])


#%%
# extraindo os codigos das selecoes no site fifaindex
fifa23 = ''
url = f'https://www.fifaindex.com/pt-br/players/{fifa23}'
req = requests.get(url)
bs_req = bs(req.text, features="lxml")
lista = bs_req.find_all('select', attrs={'name':'nationality', 'placeholder':'Select Option'})[0]

df_cod_selecoes = pd.DataFrame(columns=['cod', 'selecao'])
lista_codigos = []
lista_selecoes = []

for i in range(len(lista.find_all('option'))):
    cod = lista.find_all('option')[i]['value']
    selecao = lista.find_all('option')[i].contents[0]
    lista_codigos.append(cod)
    lista_selecoes.append(selecao)

df_cod_selecoes['cod'] = lista_codigos
df_cod_selecoes['selecao'] = lista_selecoes


#%%
# padronizando os nomes das selecoes
df_cod_selecoes['selecao'] = df_cod_selecoes['selecao'].apply(unidecode)
df_cod_selecoes['selecao'] = df_cod_selecoes['selecao'].str.lower()
df_cod_selecoes['selecao'] = df_cod_selecoes['selecao'].str.strip()
df_cod_selecoes['selecao'] = df_cod_selecoes['selecao'].str.replace(' ', '_')
df_cod_selecoes['selecao'] = df_cod_selecoes['selecao'].str.replace('catar', 'qatar')


#%%
"""
# validando quais foram as selecoes nao marcadas
df_cod_selecoes['copa_2022'].value_counts()
selecoes_esquecidas = [selecao if selecao not in list(df_cod_selecoes['selecao']) else None for selecao in selecoes_classificadas]
selecoes_esquecidas.value_counts()
"""
# marcando as selecoes classificadas para cada copa do mundo
df_cod_selecoes['copa_2022'] = [1 if selecao in selecoes_classificadas else 0 for selecao in df_cod_selecoes['selecao']]
cod_selecoes_classificadas = list(df_cod_selecoes[df_cod_selecoes['copa_2022'] == 1]['cod'])
cod_selecoes_classificadas


#%%
# extraindo as tabelas com o beautiful soup
#https://www.fifaindex.com/pt-br/players/fifa22_555/?page=5&gender=0&nationality=54&order=desc
ano_fifa = {
    'ano' : ['2023', '2019', '2015', '2011', '2007'],
    'param_url' : ['', 'fifa19_353/', 'fifa15_14/', 'fifa11_7/', 'fifa07_3/']
}
df_ano_fifa = pd.DataFrame(ano_fifa)
df_ano_fifa
#%%
for i in range(df_ano_fifa.shape[0]):
    ano = df_ano_fifa['param_url'][i]
    for selecao in cod_selecoes_classificadas:
        for page in range(1,11):
            url = f'https://www.fifaindex.com/pt-br/players/{ano}?page={page}&gender=0&nationality={selecao}&order=desc'
            req_stats = requests.get(url)
            bs_stats = bs(req_stats.text, features="lxml")
            table_stats = bs_stats.find_all('table', attrs={'class':'table table-striped table-players'})[0]
            df_stats_page_full = pd.read_html(str(table_stats))[0]
            df_stats_page = df_stats_page_full.loc[:,'GER-POT':'Idade']


"""
req_conmebol = requests.get(conmebol)
bs_conmebol = bs(req_conmebol.text, features="lxml")
table_conmebol = bs_conmebol.find_all('table', attrs={'class':'wikitable', 'style':'text-align:center;'})[0]
df_conmebol_full = pd.read_html(str(table_conmebol))[0]
df_conmebol = df_conmebol_full.loc[:,'Pos':'Classificação']
df_conmebol.to_csv('bronze/2022/df_conmebol.csv', sep=';', index=False)"""
# %%
ano = ''
page = 1
selecao = 1
url = f'https://www.fifaindex.com/pt-br/players/{ano}?page={page}&gender=0&nationality={selecao}&order=desc'
req_stats = requests.get(url)
bs_stats = bs(req_stats.text, features="lxml")
table_stats = bs_stats.find_all('table', attrs={'class':'table table-striped table-players'})[0]
df_stats_page_full = pd.read_html(str(table_stats))[0]
df_stats_page = df_stats_page_full.loc[:,'GER-POT':'Idade']
df_stats_page

# %%
df_cod_selecoes[df_cod_selecoes['selecao'] == 'alemanha']

# %%
print(url)

# %%
for page in range(1,11):
    print(page)

# %%
aaa = pd.read_csv('gold/2022/df_classif_2022.csv', sep=';')
aaa

# %%
