# TRATAMENTO DE DADOS - SILVER TO GOLD
# - criacao de medidas sobre os desclassificados
# - unificacao do dataset dos classificados
# - criacao das ultimas colunas necessarias
# - ingestao na camada gold


# importando bibliotecas
import pandas as pd
from unidecode import unidecode


# importando os dataframes da camada silver
df_conmebol_classif = pd.read_csv('silver/2022/df_conmebol_classif.csv', sep=';')
df_concacaf_classif = pd.read_csv('silver/2022/df_concacaf_classif.csv', sep=';')
df_uefa_classif = pd.read_csv('silver/2022/df_uefa_classif.csv', sep=';')
df_caf_classif = pd.read_csv('silver/2022/df_caf_classif.csv', sep=';')
df_afc_classif = pd.read_csv('silver/2022/df_afc_classif.csv', sep=';')

df_conmebol_desclassif = pd.read_csv('silver/2022/df_conmebol_desclassif.csv', sep=';')
df_concacaf_desclassif = pd.read_csv('silver/2022/df_concacaf_desclassif.csv', sep=';')
df_uefa_desclassif = pd.read_csv('silver/2022/df_uefa_desclassif.csv', sep=';')
df_caf_desclassif = pd.read_csv('silver/2022/df_caf_desclassif.csv', sep=';')
df_afc_desclassif = pd.read_csv('silver/2022/df_afc_desclassif.csv', sep=';')


# calculando as medidas das selecoes desclassificadas
list_dfs = [
    [df_conmebol_classif, df_conmebol_desclassif],
    [df_concacaf_classif, df_concacaf_desclassif],
    [df_uefa_classif, df_uefa_desclassif],
    [df_caf_classif, df_caf_desclassif],
    [df_afc_classif, df_afc_desclassif]
]

for dfs in list_dfs:
    df = dfs[0]
    df_desclassif = dfs[1]
    df['desclassif_qtd'] = df_desclassif['selecao'].value_counts().sum()
    cols = ['pts', 'v', 'e', 'd', 'gp', 'gc', 'sg']
    for col in cols:
        df[f'desclassif_{col}_min'] = round(df_desclassif[col].min(), 2)
        df[f'desclassif_{col}_max'] = round(df_desclassif[col].max(), 2)
        df[f'desclassif_{col}_mean'] = round(df_desclassif[col].mean(), 2)


# concatenando os dataframes
df_classif = pd.concat([df_conmebol_classif, df_concacaf_classif, df_uefa_classif, df_caf_classif, df_afc_classif])
df_classif


# padronizando os nomes das selecoes
df_classif['selecao'] = df_classif['selecao'].apply(unidecode)
df_classif['selecao'] = df_classif['selecao'].str.lower()
df_classif['selecao'] = df_classif['selecao'].str.strip()
df_classif['selecao'] = df_classif['selecao'].str.replace(' ', '_')
df_classif['selecao'] = df_classif['selecao'].str.replace('paises_baixos', 'holanda')


# criando as ultimas colunas necessarias
df_classif['aprov'] = round(df_classif['pts'] / (df_classif['j']*3), 2)
df_classif['perc_v'] = round(df_classif['v'] / df_classif['j'], 2)

selecoes_classif_repescagem = ['australia', 'pais_de_gales', 'costa_rica']
df_classif.loc[df_classif['selecao'].isin(selecoes_classif_repescagem),'classif'] = 'repescagem'
df_classif.loc[~df_classif['selecao'].isin(selecoes_classif_repescagem),'classif'] = 'direta'

df_classif['gp_por_jogo'] = round(df_classif['gp'] / df_classif['j'], 2)
df_classif['gc_por_jogo'] = round(df_classif['gc'] / df_classif['j'], 2)
df_classif['sg_por_jogo'] = round(df_classif['sg'] / df_classif['j'], 2)

df_classif['sede'] = None
df_classif.loc[df_classif['selecao'] == 'qatar','sede'] = 1
df_classif.loc[df_classif['selecao'] != 'qatar','sede'] = 0

for col in df_classif.columns:
    try:
        df_classif[col].fillna(round(df_classif[col].mean(),2), inplace=True)
    except:
        pass


# salvando o dataframe na camada gold
df_classif.to_csv('gold/2022/df_classif_2022.csv', sep=';', index=False)