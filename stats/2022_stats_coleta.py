# importando bibliotecas
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from unidecode import unidecode


df_classif_2022 = pd.read_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/gold/2022/df_classif_2022.csv', sep=';')
selecoes_classificadas = list(df_classif_2022['selecao'])


# extraindo os codigos das selecoes
ano = ''
url_extrair_codigo = f'https://www.fifaindex.com/pt-br/players/{ano}?page=1&gender=0&order=desc'
req = requests.get(url_extrair_codigo)
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


# padronizando os nomes das selecoes
df_cod_selecoes['selecao'] = df_cod_selecoes['selecao'].apply(unidecode)
df_cod_selecoes['selecao'] = df_cod_selecoes['selecao'].str.lower()
df_cod_selecoes['selecao'] = df_cod_selecoes['selecao'].str.strip()
df_cod_selecoes['selecao'] = df_cod_selecoes['selecao'].str.replace(' ', '_')
df_cod_selecoes['selecao'] = df_cod_selecoes['selecao'].str.replace('catar', 'qatar')
"""
# validando quais foram as selecoes nao marcadas
df_cod_selecoes['copa_2022'].value_counts()
selecoes_esquecidas = [selecao if selecao not in list(df_cod_selecoes['selecao']) else None for selecao in selecoes_classificadas]
selecoes_esquecidas.value_counts()
"""


# marcando as selecoes classificadas para cada copa do mundo
df_cod_selecoes['copa_2022'] = [1 if selecao in selecoes_classificadas else 0 for selecao in df_cod_selecoes['selecao']]
cod_selecoes_classificadas = list(df_cod_selecoes[df_cod_selecoes['copa_2022'] == 1]['cod'])


# extraindo as tabelas com o beautiful soup
"""
https://www.fifaindex.com/pt-br/players/fifa22_555/?page=5&gender=0&nationality=54&order=desc
ano_fifa = {
    'ano' : ['2023', '2019', '2015', '2011', '2007'],
    'param_url' : ['', 'fifa19_353/', 'fifa15_14/', 'fifa11_7/', 'fifa07_3/']
}
df_ano_fifa = pd.DataFrame(ano_fifa)
df_ano_fifa

for i in range(df_ano_fifa.shape[0]):
ano = df_ano_fifa['param_url'][i]
"""
contador = 0
df_stats = pd.DataFrame(columns=['GER-POT', 'Nome', 'Posições Preferidas', 'Idade', 'ano', 'page', 'selecao'])
for selecao in cod_selecoes_classificadas:
    contador += 1
    print(f'Iniciando a coleta da selecao {contador}')
    df_stats_selecao = pd.DataFrame(columns=['GER-POT', 'Nome', 'Posições Preferidas', 'Idade', 'ano', 'page', 'selecao'])

    for page in range(1,11):
        print(f'selecao {contador} / 32 - pagina {page} / 10')
        try:
            url = f'https://www.fifaindex.com/pt-br/players/{ano}?page={page}&gender=0&nationality={selecao}&order=desc'
            req_stats = requests.get(url)
            bs_stats = bs(req_stats.text, features="lxml")
            table_stats = bs_stats.find('table', attrs={'class':'table table-striped table-players'})
            df_stats_selecao_page = pd.read_html(str(table_stats))[0]
            df_stats_selecao_page = df_stats_selecao_page.loc[:,'GER-POT':'Idade']
            df_stats_selecao_page['ano'] = 2022
            df_stats_selecao_page['page'] = page
            df_stats_selecao_page['selecao'] = selecao
            df_stats_selecao = pd.concat([df_stats_selecao, df_stats_selecao_page])
        except:
            pass
    
    df_stats = pd.concat([df_stats, df_stats_selecao])

df_stats.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/2022/df_stats.csv', sep=';', index=False)