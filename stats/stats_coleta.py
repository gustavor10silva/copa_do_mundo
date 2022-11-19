# importando bibliotecas
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from unidecode import unidecode


# construindo o vetor de anos e parametros de url
ano_param_url = [
    ['2006', 'fifa07_3/'],
    ['2010', 'fifa11_7/'],
    ['2014', 'fifa15_14/'],
    ['2018', 'fifa19_353/'],
    ['2022', '']
]


# fazendo as requisicoes para cada copa
for i in range(len(ano_param_url)):
    ano = ano_param_url[i][0]
    param_url = ano_param_url[i][1]
    print(f'---------- COLETA STATS {ano} ----------')


    # importando o dataframe de classificados no respectivo ano
    df_classif= pd.read_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/gold/{ano}/df_classif_{ano}.csv', sep=';')
    selecoes_classificadas = list(df_classif['selecao'])
    print(f'STATS {ano} - {len(selecoes_classificadas)} / 32 seleções')


    # extraindo os codigos das selecoes
    url_extrair_codigo = f'https://www.fifaindex.com/pt-br/players/{param_url}?page=1&gender=0&order=desc'
    req = requests.get(url_extrair_codigo)
    bs_req = bs(req.text, features="lxml")
    lista = bs_req.find_all('select', attrs={'name':'nationality', 'placeholder':'Select Option'})[0]

    df_cod_selecoes = pd.DataFrame(columns=['cod', 'selecao'])
    lista_codigos = []
    lista_selecoes = []

    for j in range(len(lista.find_all('option'))):
        cod = lista.find_all('option')[j]['value']
        selecao = lista.find_all('option')[j].contents[0]
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
    df_cod_selecoes['selecao'] = df_cod_selecoes['selecao'].str.replace('trinidad_e_tobago', 'trindade_e_tobago')
    

    # marcando as selecoes classificadas para cada copa do mundo
    df_cod_selecoes[f'copa_{ano}'] = [1 if selecao in selecoes_classificadas else 0 for selecao in df_cod_selecoes['selecao']]
    cod_selecoes_classificadas = list(df_cod_selecoes[df_cod_selecoes[f'copa_{ano}'] == 1]['cod'])
    nome_selecoes_classificadas = list(df_cod_selecoes[df_cod_selecoes[f'copa_{ano}'] == 1]['selecao'])
    print(f'STATS {ano} - {len(cod_selecoes_classificadas)} / 32 códigos')


    # validando quais foram as selecoes nao marcadas
    if len(cod_selecoes_classificadas) != 32:
        selecoes_esquecidas = [selecao if selecao not in list(df_cod_selecoes['selecao']) else None for selecao in selecoes_classificadas]
        print(f'Seleções esquecidas: {selecoes_esquecidas}')


    # extraindo as tabelas com o beautiful soup
    contador = 0
    df_stats = pd.DataFrame(columns=['GER-POT', 'Nome', 'Posições Preferidas', 'Idade', 'ano', 'page', 'selecao'])
    for k in range(len(cod_selecoes_classificadas)):
        contador += 1
        cod_selecao = cod_selecoes_classificadas[k]
        nome_selecao = nome_selecoes_classificadas[k]
        df_stats_selecao = pd.DataFrame(columns=['GER-POT', 'Nome', 'Posições Preferidas', 'Idade', 'ano', 'page', 'selecao'])

        for page in range(1,11):
            print(f'copa {i+1} / 5 - selecao {contador} / 32 - pagina {page} / 10')
            try:
                url = f'https://www.fifaindex.com/pt-br/players/{param_url}?page={page}&gender=0&nationality={cod_selecao}&order=desc'
                req_stats = requests.get(url)
                bs_stats = bs(req_stats.text, features="lxml")
                table_stats = bs_stats.find('table', attrs={'class':'table table-striped table-players'})
                df_stats_selecao_page = pd.read_html(str(table_stats))[0]
                df_stats_selecao_page = df_stats_selecao_page.loc[:,'GER-POT':'Idade']
                df_stats_selecao_page['ano'] = ano
                df_stats_selecao_page['page'] = page
                df_stats_selecao_page['selecao'] = nome_selecao
                df_stats_selecao = pd.concat([df_stats_selecao, df_stats_selecao_page], ignore_index=True)
            except:
                pass
        
        df_stats = pd.concat([df_stats, df_stats_selecao], ignore_index=True)

    df_stats.to_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/{ano}/df_stats.csv', sep=';', index=False)