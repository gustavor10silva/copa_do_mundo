# TRATAMENTO DE DADOS - BRONZE TO SILVER
# - padronizacao dos nomes das colunas
# - criacao de colunas faltantes
# - tratamento simples e tipagem de colunas
# - separacao de dataframes por escopo (classif e desclassif)
# - ingestao na camada silver

#%%
# importando bibliotecas
import pandas as pd
from unidecode import unidecode


# importando os dataframes da camada bronze
bronze_path = 'C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/'

df_conmebol_2022 = pd.read_csv(f'{bronze_path}2022/df_conmebol.csv', sep=';')
df_concacaf_2022 = pd.read_csv(f'{bronze_path}2022/df_concacaf.csv', sep=';')
df_uefa_2022 = pd.read_csv(f'{bronze_path}2022/df_uefa.csv', sep=';')
df_caf_2022 = pd.read_csv(f'{bronze_path}2022/df_caf.csv', sep=';')
df_afc_2022 = pd.read_csv(f'{bronze_path}2022/df_afc.csv', sep=';')

df_conmebol_2018 = pd.read_csv(f'{bronze_path}2018/df_conmebol.csv', sep=';')
df_concacaf_2018 = pd.read_csv(f'{bronze_path}2018/df_concacaf.csv', sep=';')
df_uefa_2018 = pd.read_csv(f'{bronze_path}2018/df_uefa.csv', sep=';')
df_caf_2018 = pd.read_csv(f'{bronze_path}2018/df_caf.csv', sep=';')
df_afc_2018 = pd.read_csv(f'{bronze_path}2018/df_afc.csv', sep=';')

df_conmebol_2014 = pd.read_csv(f'{bronze_path}2014/df_conmebol.csv', sep=';')
df_concacaf_2014 = pd.read_csv(f'{bronze_path}2014/df_concacaf.csv', sep=';')
df_uefa_2014 = pd.read_csv(f'{bronze_path}2014/df_uefa.csv', sep=';')
df_caf_2014 = pd.read_csv(f'{bronze_path}2014/df_caf.csv', sep=';')
df_afc_2014 = pd.read_csv(f'{bronze_path}2014/df_afc.csv', sep=';')

df_conmebol_2010 = pd.read_csv(f'{bronze_path}2010/df_conmebol.csv', sep=';')
df_concacaf_2010 = pd.read_csv(f'{bronze_path}2010/df_concacaf.csv', sep=';')
df_uefa_2010 = pd.read_csv(f'{bronze_path}2010/df_uefa.csv', sep=';')
df_caf_2010 = pd.read_csv(f'{bronze_path}2010/df_caf.csv', sep=';')
df_afc_2010 = pd.read_csv(f'{bronze_path}2010/df_afc.csv', sep=';')
df_ofc_2010 = pd.read_csv(f'{bronze_path}2010/df_ofc.csv', sep=';')

df_conmebol_2006 = pd.read_csv(f'{bronze_path}2006/df_conmebol.csv', sep=';')
df_concacaf_2006 = pd.read_csv(f'{bronze_path}2006/df_concacaf.csv', sep=';')
df_uefa_2006 = pd.read_csv(f'{bronze_path}2006/df_uefa.csv', sep=';')
df_caf_2006 = pd.read_csv(f'{bronze_path}2006/df_caf.csv', sep=';')
df_afc_2006 = pd.read_csv(f'{bronze_path}2006/df_afc.csv', sep=';')
df_ofc_2006 = pd.read_csv(f'{bronze_path}2006/df_ofc.csv', sep=';')


# criando listas com os dataframes
dfs_conmebol = [df_conmebol_2022, df_conmebol_2018, df_conmebol_2014, df_conmebol_2010, df_conmebol_2006]
dfs_concacaf = [df_concacaf_2022, df_concacaf_2018, df_concacaf_2014, df_concacaf_2010, df_concacaf_2006]
dfs_uefa = [df_uefa_2022, df_uefa_2018, df_uefa_2014, df_uefa_2010, df_uefa_2006]
dfs_caf = [df_caf_2022, df_caf_2018, df_caf_2014, df_caf_2010, df_caf_2006]
dfs_afc = [df_afc_2022, df_afc_2018, df_afc_2014, df_afc_2010, df_afc_2006]
dfs_ofc = [df_ofc_2010, df_ofc_2006]

dfs = dfs_conmebol + dfs_concacaf + dfs_uefa + dfs_caf + dfs_afc + dfs_ofc

#%%
# padronizando os nomes das colunas
dict_rename = {
    'P':'pos_elim',
    'Pos':'pos_elim',
    'Pos.':'pos_elim',
    'Equipe':'selecao',
    'vdeSeleção':'selecao',
    'Seleção':'selecao',
    'País':'selecao',
    'Grupo A':'selecao',
    'Grupo B':'selecao',
    'Pts':'pts',
    'Pts.':'pts',
    'J':'j',
    'V':'v',
    'E':'e',
    'D':'d',
    'GP':'gp',
    'GC':'gc',
    'SG':'sg',
    'Classificação':'classif'
}

for df in dfs:
    df.rename(columns=dict_rename, inplace=True)

#%%
# criando a coluna de confederacao continental
for df in dfs_conmebol:
    df['conf_cont'] = 'conmebol'

for df in dfs_concacaf:
    df['conf_cont'] = 'concacaf'

for df in dfs_uefa:
    df['conf_cont'] = 'uefa'

for df in dfs_caf:
    df['conf_cont'] = 'caf'

for df in dfs_afc:
    df['conf_cont'] = 'afc'

for df in dfs_ofc:
    df['conf_cont'] = 'ofc'

#%%
# tratamento simples e tipagem das colunas
for df in dfs:
    df['pos_elim'] = df['pos_elim'].replace('°', '', regex=True)

    df['selecao'] = df['selecao'].apply(unidecode)
    df['selecao'] = df['selecao'].str.lower()
    df['selecao'] = df['selecao'].str.replace('\*', '', regex=True)
    df['selecao'] = df['selecao'].str.strip()
    df['selecao'] = df['selecao'].str.replace(' ', '_')
    df['selecao'] = df['selecao'].str.replace('paises_baixos', 'holanda')

    if 'sg' not in df.columns:
        df['sg'] = df['gp'] - df['gc']
    
    df['sg'] = df['sg'].replace('\+', '', regex=True)
    df['sg'] = df['sg'].replace('−', '-', regex=True)
    df['sg'] = df['sg'].replace('–', '-', regex=True)

    if 'classif' in df.columns:
        df.drop(columns=['classif'], inplace=True)

    for col in ['pos_elim', 'pts', 'j', 'v', 'e', 'd', 'gp', 'gc', 'sg']:
        df[col] = df[col].astype(int)


#%%
# separacao de dataframes por escopo (classif e desclassif)
selecoes_classif_2022 = [
    'qatar', 'equador', 'senegal', 'holanda', 'inglaterra', 'ira', 'estados_unidos', 'pais_de_gales',
    'argentina', 'arabia_saudita', 'mexico', 'polonia', 'franca', 'australia', 'dinamarca', 'tunisia',
    'espanha', 'costa_rica', 'alemanha', 'japao', 'belgica', 'canada', 'marrocos', 'croacia',
    'brasil', 'servia', 'suica', 'camaroes', 'portugal', 'gana', 'uruguai', 'coreia_do_sul'
    ]

selecoes_classif_2018 = [
    'russia', 'alemanha', 'brasil', 'portugal', 'argentina', 'belgica', 'polonia', 'franca',
    'espanha', 'peru', 'suica', 'inglaterra', 'colombia', 'mexico', 'uruguai', 'croacia',
    'dinamarca', 'islandia', 'costa_rica', 'suecia', 'tunisia', 'egito', 'senegal', 'ira',
    'servia', 'nigeria', 'australia', 'japao', 'marrocos', 'panama', 'coreia_do_sul', 'arabia_saudita'
]

selecoes_classif_2014 = [
    'brasil', 'argentina', 'colombia', 'chile', 'equador', 'uruguai', 'estados_unidos', 'costa_rica', 
    'honduras', 'mexico', 'coreia_do_sul', 'japao', 'ira', 'australia', 'nigeria', 'costa_do_marfim',
    'camaroes', 'gana', 'argelia','italia', 'espanha', 'belgica', 'holanda', 'inglaterra', 'alemanha',
    'russia', 'suica', 'bosnia', 'franca', 'portugal','grecia', 'croacia'
]

selecoes_classif_2010 = [
    'uruguai', 'argentina', 'estados_unidos', 'alemanha', 'mexico', 'coreia_do_sul', 'inglaterra', 'gana',
    'africa_do_sul', 'grecia', 'eslovenia', 'australia', 'franca', 'nigeria', 'argelia', 'servia',
    'holanda', 'paraguai', 'brasil', 'espanha', 'dinamarca', 'eslovaquia', 'portugal', 'chile',
    'japao', 'nova_zelandia', 'costa_do_marfim', 'suica', 'camaroes', 'italia', 'coreia_do_norte', 'honduras'
]

selecoes_classif_2006 = [
    'alemanha', 'inglaterra', 'argentina', 'portugal', 'equador', 'suecia', 'holanda', 'mexico',
    'polonia', 'paraguai', 'costa_do_marfim', 'angola', 'costa_rica', 'trinidad_e_tobago', 'servia_e_montenegro',
    'ira', 'italia', 'brasil', 'suica', 'espanha', 'gana', 'australia', 'franca', 'ucrania', 'republica_tcheca',
    'croacia', 'coreia_do_sul', 'tunisia','estados_unidos', 'japao', 'togo', 'arabia_saudita'
]

#%%
df_conmebol_desclassif = df_conmebol[~df_conmebol['selecao'].isin(selecoes_classificadas)]
df_conmebol_classif = df_conmebol[df_conmebol['selecao'].isin(selecoes_classificadas)]

df_concacaf_desclassif = df_concacaf[~df_concacaf['selecao'].isin(selecoes_classificadas)]
df_concacaf_classif = df_concacaf[df_concacaf['selecao'].isin(selecoes_classificadas)]

df_uefa_desclassif = df_uefa[~df_uefa['selecao'].isin(selecoes_classificadas)]
df_uefa_classif = df_uefa[df_uefa['selecao'].isin(selecoes_classificadas)]

df_caf_desclassif = df_caf[~df_caf['selecao'].isin(selecoes_classificadas)]
df_caf_classif = df_caf[df_caf['selecao'].isin(selecoes_classificadas)]

df_afc_desclassif = df_afc[~df_afc['selecao'].isin(selecoes_classificadas)]
df_afc_classif = df_afc[df_afc['selecao'].isin(selecoes_classificadas)]
list_qatar = [None, 'Qatar'] + [None]*(df_afc_classif.shape[1]-2)
df_afc_classif.loc[len(df_afc_classif)] = list_qatar


# validacao simples do numero de selecoes classificadas
qtd_conmebol = 4
qtd_concacaf = 4
qtd_uefa = 13
qtd_caf = 5
qtd_afc = 6
qtd_total_esperada = qtd_conmebol + qtd_concacaf + qtd_uefa + qtd_caf + qtd_afc
qtd_total_atual = len(df_conmebol_classif) + len(df_concacaf_classif) + len(df_uefa_classif) + len(df_caf_classif) + len(df_afc_classif)

vetor_valid = [
    ['conmebol', qtd_conmebol, len(df_conmebol_classif)],
    ['concacaf', qtd_concacaf, len(df_concacaf_classif)],
    ['uefa', qtd_uefa, len(df_uefa_classif)],
    ['caf', qtd_caf, len(df_caf_classif)],
    ['afc', qtd_afc, len(df_afc_classif)],
    ['total', qtd_total_esperada, qtd_total_atual]
]
for valid in vetor_valid:
    print(f'Qtd {valid[0]}: {valid[2]} de {valid[1]} selecoes classif')


# salvando os dataframes
df_conmebol_classif.to_csv('silver/2022/df_conmebol_classif.csv', sep=';', index=False)
df_concacaf_classif.to_csv('silver/2022/df_concacaf_classif.csv', sep=';', index=False)
df_uefa_classif.to_csv('silver/2022/df_uefa_classif.csv', sep=';', index=False)
df_caf_classif.to_csv('silver/2022/df_caf_classif.csv', sep=';', index=False)
df_afc_classif.to_csv('silver/2022/df_afc_classif.csv', sep=';', index=False)

df_conmebol_desclassif.to_csv('silver/2022/df_conmebol_desclassif.csv', sep=';', index=False)
df_concacaf_desclassif.to_csv('silver/2022/df_concacaf_desclassif.csv', sep=';', index=False)
df_uefa_desclassif.to_csv('silver/2022/df_uefa_desclassif.csv', sep=';', index=False)
df_caf_desclassif.to_csv('silver/2022/df_caf_desclassif.csv', sep=';', index=False)
df_afc_desclassif.to_csv('silver/2022/df_afc_desclassif.csv', sep=';', index=False)
# %%
