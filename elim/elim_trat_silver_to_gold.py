# TRATAMENTO DE DADOS - SILVER TO GOLD
# - criacao de medidas sobre os desclassificados
# - unificacao do dataset dos classificados
# - criacao das ultimas colunas necessarias
# - ingestao na camada gold


# importando bibliotecas
import pandas as pd
from unidecode import unidecode


# importando os dataframes da camada silver
silver_path = 'C:/Users/Rustabo/Projetos/copa_do_mundo/silver/'

# importando os dataframes das selecoes classificadas
df_conmebol_2022_classif = pd.read_csv(f'{silver_path}2022/df_conmebol_2022_classif.csv', sep=';')
df_concacaf_2022_classif = pd.read_csv(f'{silver_path}2022/df_concacaf_2022_classif.csv', sep=';')
df_uefa_2022_classif = pd.read_csv(f'{silver_path}2022/df_uefa_2022_classif.csv', sep=';')
df_caf_2022_classif = pd.read_csv(f'{silver_path}2022/df_caf_2022_classif.csv', sep=';')
df_afc_2022_classif = pd.read_csv(f'{silver_path}2022/df_afc_2022_classif.csv', sep=';')

df_conmebol_2018_classif = pd.read_csv(f'{silver_path}2018/df_conmebol_2018_classif.csv', sep=';')
df_concacaf_2018_classif = pd.read_csv(f'{silver_path}2018/df_concacaf_2018_classif.csv', sep=';')
df_uefa_2018_classif = pd.read_csv(f'{silver_path}2018/df_uefa_2018_classif.csv', sep=';')
df_caf_2018_classif = pd.read_csv(f'{silver_path}2018/df_caf_2018_classif.csv', sep=';')
df_afc_2018_classif = pd.read_csv(f'{silver_path}2018/df_afc_2018_classif.csv', sep=';')

df_conmebol_2014_classif = pd.read_csv(f'{silver_path}2014/df_conmebol_2014_classif.csv', sep=';')
df_concacaf_2014_classif = pd.read_csv(f'{silver_path}2014/df_concacaf_2014_classif.csv', sep=';')
df_uefa_2014_classif = pd.read_csv(f'{silver_path}2014/df_uefa_2014_classif.csv', sep=';')
df_caf_2014_classif = pd.read_csv(f'{silver_path}2014/df_caf_2014_classif.csv', sep=';')
df_afc_2014_classif = pd.read_csv(f'{silver_path}2014/df_afc_2014_classif.csv', sep=';')

df_conmebol_2010_classif = pd.read_csv(f'{silver_path}2010/df_conmebol_2010_classif.csv', sep=';')
df_concacaf_2010_classif = pd.read_csv(f'{silver_path}2010/df_concacaf_2010_classif.csv', sep=';')
df_uefa_2010_classif = pd.read_csv(f'{silver_path}2010/df_uefa_2010_classif.csv', sep=';')
df_caf_2010_classif = pd.read_csv(f'{silver_path}2010/df_caf_2010_classif.csv', sep=';')
df_afc_2010_classif = pd.read_csv(f'{silver_path}2010/df_afc_2010_classif.csv', sep=';')
df_ofc_2010_classif = pd.read_csv(f'{silver_path}2010/df_ofc_2010_classif.csv', sep=';')

df_conmebol_2006_classif = pd.read_csv(f'{silver_path}2006/df_conmebol_2006_classif.csv', sep=';')
df_concacaf_2006_classif = pd.read_csv(f'{silver_path}2006/df_concacaf_2006_classif.csv', sep=';')
df_uefa_2006_classif = pd.read_csv(f'{silver_path}2006/df_uefa_2006_classif.csv', sep=';')
df_caf_2006_classif = pd.read_csv(f'{silver_path}2006/df_caf_2006_classif.csv', sep=';')
df_afc_2006_classif = pd.read_csv(f'{silver_path}2006/df_afc_2006_classif.csv', sep=';')
df_ofc_2006_classif = pd.read_csv(f'{silver_path}2006/df_ofc_2006_classif.csv', sep=';')


# importando os dataframes das selecoes desclassificadas
df_conmebol_2022_desclassif = pd.read_csv(f'{silver_path}2022/df_conmebol_2022_desclassif.csv', sep=';')
df_concacaf_2022_desclassif = pd.read_csv(f'{silver_path}2022/df_concacaf_2022_desclassif.csv', sep=';')
df_uefa_2022_desclassif = pd.read_csv(f'{silver_path}2022/df_uefa_2022_desclassif.csv', sep=';')
df_caf_2022_desclassif = pd.read_csv(f'{silver_path}2022/df_caf_2022_desclassif.csv', sep=';')
df_afc_2022_desclassif = pd.read_csv(f'{silver_path}2022/df_afc_2022_desclassif.csv', sep=';')

df_conmebol_2018_desclassif = pd.read_csv(f'{silver_path}2018/df_conmebol_2018_desclassif.csv', sep=';')
df_concacaf_2018_desclassif = pd.read_csv(f'{silver_path}2018/df_concacaf_2018_desclassif.csv', sep=';')
df_uefa_2018_desclassif = pd.read_csv(f'{silver_path}2018/df_uefa_2018_desclassif.csv', sep=';')
df_caf_2018_desclassif = pd.read_csv(f'{silver_path}2018/df_caf_2018_desclassif.csv', sep=';')
df_afc_2018_desclassif = pd.read_csv(f'{silver_path}2018/df_afc_2018_desclassif.csv', sep=';')

df_conmebol_2014_desclassif = pd.read_csv(f'{silver_path}2014/df_conmebol_2014_desclassif.csv', sep=';')
df_concacaf_2014_desclassif = pd.read_csv(f'{silver_path}2014/df_concacaf_2014_desclassif.csv', sep=';')
df_uefa_2014_desclassif = pd.read_csv(f'{silver_path}2014/df_uefa_2014_desclassif.csv', sep=';')
df_caf_2014_desclassif = pd.read_csv(f'{silver_path}2014/df_caf_2014_desclassif.csv', sep=';')
df_afc_2014_desclassif = pd.read_csv(f'{silver_path}2014/df_afc_2014_desclassif.csv', sep=';')

df_conmebol_2010_desclassif = pd.read_csv(f'{silver_path}2010/df_conmebol_2010_desclassif.csv', sep=';')
df_concacaf_2010_desclassif = pd.read_csv(f'{silver_path}2010/df_concacaf_2010_desclassif.csv', sep=';')
df_uefa_2010_desclassif = pd.read_csv(f'{silver_path}2010/df_uefa_2010_desclassif.csv', sep=';')
df_caf_2010_desclassif = pd.read_csv(f'{silver_path}2010/df_caf_2010_desclassif.csv', sep=';')
df_afc_2010_desclassif = pd.read_csv(f'{silver_path}2010/df_afc_2010_desclassif.csv', sep=';')
df_ofc_2010_desclassif = pd.read_csv(f'{silver_path}2010/df_ofc_2010_desclassif.csv', sep=';')

df_conmebol_2006_desclassif = pd.read_csv(f'{silver_path}2006/df_conmebol_2006_desclassif.csv', sep=';')
df_concacaf_2006_desclassif = pd.read_csv(f'{silver_path}2006/df_concacaf_2006_desclassif.csv', sep=';')
df_uefa_2006_desclassif = pd.read_csv(f'{silver_path}2006/df_uefa_2006_desclassif.csv', sep=';')
df_caf_2006_desclassif = pd.read_csv(f'{silver_path}2006/df_caf_2006_desclassif.csv', sep=';')
df_afc_2006_desclassif = pd.read_csv(f'{silver_path}2006/df_afc_2006_desclassif.csv', sep=';')
df_ofc_2006_desclassif = pd.read_csv(f'{silver_path}2006/df_ofc_2006_desclassif.csv', sep=';')


# calculando as medidas das selecoes desclassificadas
list_dfs = [
    [df_conmebol_2022_classif, df_conmebol_2022_desclassif],
    [df_concacaf_2022_classif, df_concacaf_2022_desclassif],
    [df_uefa_2022_classif, df_uefa_2022_desclassif],
    [df_caf_2022_classif, df_caf_2022_desclassif],
    [df_afc_2022_classif, df_afc_2022_desclassif],
    [df_conmebol_2018_classif, df_conmebol_2018_desclassif],
    [df_concacaf_2018_classif, df_concacaf_2018_desclassif],
    [df_uefa_2018_classif, df_uefa_2018_desclassif],
    [df_caf_2018_classif, df_caf_2018_desclassif],
    [df_afc_2018_classif, df_afc_2018_desclassif],
    [df_conmebol_2014_classif, df_conmebol_2014_desclassif],
    [df_concacaf_2014_classif, df_concacaf_2014_desclassif],
    [df_uefa_2014_classif, df_uefa_2014_desclassif],
    [df_caf_2014_classif, df_caf_2014_desclassif],
    [df_afc_2014_classif, df_afc_2014_desclassif],
    [df_conmebol_2010_classif, df_conmebol_2010_desclassif],
    [df_concacaf_2010_classif, df_concacaf_2010_desclassif],
    [df_uefa_2010_classif, df_uefa_2010_desclassif],
    [df_caf_2010_classif, df_caf_2010_desclassif],
    [df_afc_2010_classif, df_afc_2010_desclassif],
    [df_ofc_2010_classif, df_ofc_2010_desclassif],
    [df_conmebol_2006_classif, df_conmebol_2006_desclassif],
    [df_concacaf_2006_classif, df_concacaf_2006_desclassif],
    [df_uefa_2006_classif, df_uefa_2006_desclassif],
    [df_caf_2006_classif, df_caf_2006_desclassif],
    [df_afc_2006_classif, df_afc_2006_desclassif],
    [df_ofc_2006_classif, df_ofc_2006_desclassif]
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
df_classif_2022 = pd.concat([df_conmebol_2022_classif, df_concacaf_2022_classif, df_uefa_2022_classif, df_caf_2022_classif, df_afc_2022_classif], ignore_index=True)
df_classif_2018 = pd.concat([df_conmebol_2018_classif, df_concacaf_2018_classif, df_uefa_2018_classif, df_caf_2018_classif, df_afc_2018_classif], ignore_index=True)
df_classif_2014 = pd.concat([df_conmebol_2014_classif, df_concacaf_2014_classif, df_uefa_2014_classif, df_caf_2014_classif, df_afc_2014_classif], ignore_index=True)
df_classif_2010 = pd.concat([df_conmebol_2010_classif, df_concacaf_2010_classif, df_uefa_2010_classif, df_caf_2010_classif, df_afc_2010_classif, df_ofc_2010_classif], ignore_index=True)
df_classif_2006 = pd.concat([df_conmebol_2006_classif, df_concacaf_2006_classif, df_uefa_2006_classif, df_caf_2006_classif, df_afc_2006_classif, df_ofc_2006_classif], ignore_index=True)


# criando as ultimas colunas necessarias
lista_df_classif = [df_classif_2022, df_classif_2018, df_classif_2014, df_classif_2010, df_classif_2006]

selecoes_classif_repescagem = [
        ['australia', 'costa_rica'],
        ['australia', 'peru'],
        ['uruguai', 'mexico'],
        ['nova_zelandia', 'uruguai'],
        ['australia', 'trindade_e_tobago']
        ]

sede = ['qatar', 'russia', 'brasil', 'africa_do_sul', 'alemanha']

for i in range(len(lista_df_classif)):
    df = lista_df_classif[i]
    df['aprov'] = round(df['pts'] / (df['j']*3), 2)
    df['perc_v'] = round(df['v'] / df['j'], 2)

    df['repescagem'] = None
    df.loc[df['selecao'].isin(selecoes_classif_repescagem[i]),'repescagem'] = 1
    df.loc[~df['selecao'].isin(selecoes_classif_repescagem[i]),'repescagem'] = 0

    df['gp_por_jogo'] = round(df['gp'] / df['j'], 2)
    df['gc_por_jogo'] = round(df['gc'] / df['j'], 2)
    df['sg_por_jogo'] = round(df['sg'] / df['j'], 2)

    df['sede'] = None
    df.loc[df['selecao'] == sede[i],'sede'] = 1
    df.loc[df['selecao'] != sede[i],'sede'] = 0

    for col in df.columns:
        try:
            df[col].fillna(round(df[col].mean(),2), inplace=True)
        except:
            pass


# salvando os dataframes na camada gold
gold_path = 'C:/Users/Rustabo/Projetos/copa_do_mundo/gold/'

df_classif_2022.to_csv(f'{gold_path}2022/df_classif_2022.csv', sep=';', index=False)
df_classif_2018.to_csv(f'{gold_path}2018/df_classif_2018.csv', sep=';', index=False)
df_classif_2014.to_csv(f'{gold_path}2014/df_classif_2014.csv', sep=';', index=False)
df_classif_2010.to_csv(f'{gold_path}2010/df_classif_2010.csv', sep=';', index=False)
df_classif_2006.to_csv(f'{gold_path}2006/df_classif_2006.csv', sep=';', index=False)