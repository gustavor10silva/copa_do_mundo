# TRATAMENTO DE DADOS - BRONZE TO SILVER
# - padronizacao dos nomes das colunas
# - criacao de colunas faltantes
# - tratamento simples e tipagem de colunas
# - separacao de dataframes por escopo (classif e desclassif)
# - ingestao na camada silver


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


# tratamento simples e tipagem das colunas
for df in dfs:
    df['pos_elim'] = df['pos_elim'].replace('°', '', regex=True)

    df['selecao'] = df['selecao'].apply(unidecode)
    df['selecao'] = df['selecao'].str.lower()
    df['selecao'] = df['selecao'].str.replace('\*', '', regex=True)
    df['selecao'] = df['selecao'].str.strip()
    df['selecao'] = df['selecao'].str.replace(' ', '_')
    df['selecao'] = df['selecao'].str.replace('paises_baixos', 'holanda')
    df['selecao'] = df['selecao'].str.replace('chequia', 'republica_tcheca')
    df['selecao'] = df['selecao'].str.replace('servia_e_montenegro', 'servia')

    if 'sg' not in df.columns:
        df['sg'] = df['gp'] - df['gc']
    
    df['sg'] = df['sg'].replace('\+', '', regex=True)
    df['sg'] = df['sg'].replace('−', '-', regex=True)
    df['sg'] = df['sg'].replace('–', '-', regex=True)

    if 'classif' in df.columns:
        df.drop(columns=['classif'], inplace=True)

    for col in ['pos_elim', 'pts', 'j', 'v', 'e', 'd', 'gp', 'gc', 'sg']:
        df[col] = df[col].astype(int)


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
    'russia', 'suica', 'bosnia_e_herzegovina', 'franca', 'portugal','grecia', 'croacia'
]
selecoes_classif_2010 = [
    'uruguai', 'argentina', 'estados_unidos', 'alemanha', 'mexico', 'coreia_do_sul', 'inglaterra', 'gana',
    'africa_do_sul', 'grecia', 'eslovenia', 'australia', 'franca', 'nigeria', 'argelia', 'servia',
    'holanda', 'paraguai', 'brasil', 'espanha', 'dinamarca', 'eslovaquia', 'portugal', 'chile',
    'japao', 'nova_zelandia', 'costa_do_marfim', 'suica', 'camaroes', 'italia', 'coreia_do_norte', 'honduras'
]
selecoes_classif_2006 = [
    'alemanha', 'inglaterra', 'argentina', 'portugal', 'equador', 'suecia', 'holanda', 'mexico',
    'polonia', 'paraguai', 'costa_do_marfim', 'angola', 'costa_rica', 'trindade_e_tobago', 'servia',
    'ira', 'italia', 'brasil', 'suica', 'espanha', 'gana', 'australia', 'franca', 'ucrania', 'republica_tcheca',
    'croacia', 'coreia_do_sul', 'tunisia','estados_unidos', 'japao', 'togo', 'arabia_saudita'
]


# dataframes das selecoes classificadas
df_conmebol_2022_classif = df_conmebol_2022[df_conmebol_2022['selecao'].isin(selecoes_classif_2022)]
df_concacaf_2022_classif = df_concacaf_2022[df_concacaf_2022['selecao'].isin(selecoes_classif_2022)]
df_uefa_2022_classif = df_uefa_2022[df_uefa_2022['selecao'].isin(selecoes_classif_2022)]
df_caf_2022_classif = df_caf_2022[df_caf_2022['selecao'].isin(selecoes_classif_2022)]
df_afc_2022_classif = df_afc_2022[df_afc_2022['selecao'].isin(selecoes_classif_2022)]

df_conmebol_2018_classif = df_conmebol_2018[df_conmebol_2018['selecao'].isin(selecoes_classif_2018)]
df_concacaf_2018_classif = df_concacaf_2018[df_concacaf_2018['selecao'].isin(selecoes_classif_2018)]
df_uefa_2018_classif = df_uefa_2018[df_uefa_2018['selecao'].isin(selecoes_classif_2018)]
df_caf_2018_classif = df_caf_2018[df_caf_2018['selecao'].isin(selecoes_classif_2018)]
df_afc_2018_classif = df_afc_2018[df_afc_2018['selecao'].isin(selecoes_classif_2018)]

df_conmebol_2014_classif = df_conmebol_2014[df_conmebol_2014['selecao'].isin(selecoes_classif_2014)]
df_concacaf_2014_classif = df_concacaf_2014[df_concacaf_2014['selecao'].isin(selecoes_classif_2014)]
df_uefa_2014_classif = df_uefa_2014[df_uefa_2014['selecao'].isin(selecoes_classif_2014)]
df_caf_2014_classif = df_caf_2014[df_caf_2014['selecao'].isin(selecoes_classif_2014)]
df_afc_2014_classif = df_afc_2014[df_afc_2014['selecao'].isin(selecoes_classif_2014)]

df_conmebol_2010_classif = df_conmebol_2010[df_conmebol_2010['selecao'].isin(selecoes_classif_2010)]
df_concacaf_2010_classif = df_concacaf_2010[df_concacaf_2010['selecao'].isin(selecoes_classif_2010)]
df_uefa_2010_classif = df_uefa_2010[df_uefa_2010['selecao'].isin(selecoes_classif_2010)]
df_caf_2010_classif = df_caf_2010[df_caf_2010['selecao'].isin(selecoes_classif_2010)]
df_afc_2010_classif = df_afc_2010[df_afc_2010['selecao'].isin(selecoes_classif_2010)]
df_ofc_2010_classif = df_ofc_2010[df_ofc_2010['selecao'].isin(selecoes_classif_2010)]

df_conmebol_2006_classif = df_conmebol_2006[df_conmebol_2006['selecao'].isin(selecoes_classif_2006)]
df_concacaf_2006_classif = df_concacaf_2006[df_concacaf_2006['selecao'].isin(selecoes_classif_2006)]
df_uefa_2006_classif = df_uefa_2006[df_uefa_2006['selecao'].isin(selecoes_classif_2006)]
df_caf_2006_classif = df_caf_2006[df_caf_2006['selecao'].isin(selecoes_classif_2006)]
df_afc_2006_classif = df_afc_2006[df_afc_2006['selecao'].isin(selecoes_classif_2006)]
df_ofc_2006_classif = df_ofc_2006[df_ofc_2006['selecao'].isin(selecoes_classif_2006)]


# dataframes das selecoes desclassificadas
df_conmebol_2022_desclassif = df_conmebol_2022[~df_conmebol_2022['selecao'].isin(selecoes_classif_2022)]
df_concacaf_2022_desclassif = df_concacaf_2022[~df_concacaf_2022['selecao'].isin(selecoes_classif_2022)]
df_uefa_2022_desclassif = df_uefa_2022[~df_uefa_2022['selecao'].isin(selecoes_classif_2022)]
df_caf_2022_desclassif = df_caf_2022[~df_caf_2022['selecao'].isin(selecoes_classif_2022)]
df_afc_2022_desclassif = df_afc_2022[~df_afc_2022['selecao'].isin(selecoes_classif_2022)]

df_conmebol_2018_desclassif = df_conmebol_2018[~df_conmebol_2018['selecao'].isin(selecoes_classif_2018)]
df_concacaf_2018_desclassif = df_concacaf_2018[~df_concacaf_2018['selecao'].isin(selecoes_classif_2018)]
df_uefa_2018_desclassif = df_uefa_2018[~df_uefa_2018['selecao'].isin(selecoes_classif_2018)]
df_caf_2018_desclassif = df_caf_2018[~df_caf_2018['selecao'].isin(selecoes_classif_2018)]
df_afc_2018_desclassif = df_afc_2018[~df_afc_2018['selecao'].isin(selecoes_classif_2018)]

df_conmebol_2014_desclassif = df_conmebol_2014[~df_conmebol_2014['selecao'].isin(selecoes_classif_2014)]
df_concacaf_2014_desclassif = df_concacaf_2014[~df_concacaf_2014['selecao'].isin(selecoes_classif_2014)]
df_uefa_2014_desclassif = df_uefa_2014[~df_uefa_2014['selecao'].isin(selecoes_classif_2014)]
df_caf_2014_desclassif = df_caf_2014[~df_caf_2014['selecao'].isin(selecoes_classif_2014)]
df_afc_2014_desclassif = df_afc_2014[~df_afc_2014['selecao'].isin(selecoes_classif_2014)]

df_conmebol_2010_desclassif = df_conmebol_2010[~df_conmebol_2010['selecao'].isin(selecoes_classif_2010)]
df_concacaf_2010_desclassif = df_concacaf_2010[~df_concacaf_2010['selecao'].isin(selecoes_classif_2010)]
df_uefa_2010_desclassif = df_uefa_2010[~df_uefa_2010['selecao'].isin(selecoes_classif_2010)]
df_caf_2010_desclassif = df_caf_2010[~df_caf_2010['selecao'].isin(selecoes_classif_2010)]
df_afc_2010_desclassif = df_afc_2010[~df_afc_2010['selecao'].isin(selecoes_classif_2010)]
df_ofc_2010_desclassif = df_ofc_2010[~df_ofc_2010['selecao'].isin(selecoes_classif_2010)]

df_conmebol_2006_desclassif = df_conmebol_2006[~df_conmebol_2006['selecao'].isin(selecoes_classif_2006)]
df_concacaf_2006_desclassif = df_concacaf_2006[~df_concacaf_2006['selecao'].isin(selecoes_classif_2006)]
df_uefa_2006_desclassif = df_uefa_2006[~df_uefa_2006['selecao'].isin(selecoes_classif_2006)]
df_caf_2006_desclassif = df_caf_2006[~df_caf_2006['selecao'].isin(selecoes_classif_2006)]
df_afc_2006_desclassif = df_afc_2006[~df_afc_2006['selecao'].isin(selecoes_classif_2006)]
df_ofc_2006_desclassif = df_ofc_2006[~df_ofc_2006['selecao'].isin(selecoes_classif_2006)]


# adicionando o pais sede da copa
df_afc_2022_classif.reset_index(drop=True, inplace=True)
df_afc_2022_classif.loc[len(df_afc_2022_classif)] = None
df_afc_2022_classif['selecao'].loc[df_afc_2022_classif.shape[0] - 1]  = 'qatar'
df_afc_2022_classif['conf_cont'].loc[df_afc_2022_classif.shape[0] - 1] = 'afc'

df_uefa_2018_classif.reset_index(drop=True, inplace=True)
df_uefa_2018_classif.loc[len(df_uefa_2018_classif)] = None
df_uefa_2018_classif['selecao'].loc[df_uefa_2018_classif.shape[0] - 1]  = 'russia'
df_uefa_2018_classif['conf_cont'].loc[df_uefa_2018_classif.shape[0] - 1] = 'uefa'

df_conmebol_2014_classif.reset_index(drop=True, inplace=True)
df_conmebol_2014_classif.loc[len(df_conmebol_2014_classif)] = None
df_conmebol_2014_classif['selecao'].loc[df_conmebol_2014_classif.shape[0] - 1]  = 'brasil'
df_conmebol_2014_classif['conf_cont'].loc[df_conmebol_2014_classif.shape[0] - 1] = 'conmebol'

df_caf_2010_classif.reset_index(drop=True, inplace=True)
df_caf_2010_classif.loc[len(df_caf_2010_classif)] = None
df_caf_2010_classif['selecao'].loc[df_caf_2010_classif.shape[0] - 1]  = 'africa_do_sul'
df_caf_2010_classif['conf_cont'].loc[df_caf_2010_classif.shape[0] - 1] = 'caf'

df_uefa_2006_classif.reset_index(drop=True, inplace=True)
df_uefa_2006_classif.loc[len(df_uefa_2006_classif)] = None
df_uefa_2006_classif['selecao'].loc[df_uefa_2006_classif.shape[0] - 1]  = 'alemanha'
df_uefa_2006_classif['conf_cont'].loc[df_uefa_2006_classif.shape[0] - 1] = 'uefa'


# validacao simples do numero de selecoes classificadas
dfs_classif_2022 = [df_conmebol_2022_classif, df_concacaf_2022_classif, df_uefa_2022_classif, df_caf_2022_classif, df_afc_2022_classif]
dfs_classif_2018 = [df_conmebol_2018_classif, df_concacaf_2018_classif, df_uefa_2018_classif, df_caf_2018_classif, df_afc_2018_classif]
dfs_classif_2014 = [df_conmebol_2014_classif, df_concacaf_2014_classif, df_uefa_2014_classif, df_caf_2014_classif, df_afc_2014_classif]
dfs_classif_2010 = [df_conmebol_2010_classif, df_concacaf_2010_classif, df_uefa_2010_classif, df_caf_2010_classif, df_afc_2010_classif, df_ofc_2010_classif]
dfs_classif_2006 = [df_conmebol_2006_classif, df_concacaf_2006_classif, df_uefa_2006_classif, df_caf_2006_classif, df_afc_2006_classif, df_ofc_2006_classif]

dfs_classif = [dfs_classif_2022, dfs_classif_2018, dfs_classif_2014, dfs_classif_2010, dfs_classif_2006]
listas_selecoes_classif = [selecoes_classif_2022, selecoes_classif_2018, selecoes_classif_2014, selecoes_classif_2010, selecoes_classif_2006]

for i in range(len(dfs_classif)):
    contador = 0
    df_selecoes_classif = pd.DataFrame(columns=df_conmebol_2022_classif.columns)

    for df_conf_cont in dfs_classif[i]:
        contador += df_conf_cont.shape[0]
        df_selecoes_classif = pd.concat([df_selecoes_classif, df_conf_cont])

    ano_copa = 2022 - i * 4
    selecoes_classif = listas_selecoes_classif[i]
    selecoes_faltantes = [selecao for selecao in selecoes_classif if selecao not in list(df_selecoes_classif['selecao'])]

    print(f'Temos {contador} seleções no df da copa de {ano_copa}')
    print(f'Faltam as seleções: {selecoes_faltantes}')
    print('')


# salvando os dataframes

silver_path = 'C:/Users/Rustabo/Projetos/copa_do_mundo/silver/'

# salvando os dataframes das selecoes classificadas
df_conmebol_2022_classif.to_csv(f'{silver_path}2022/df_conmebol_2022_classif.csv', sep=';', index=False)
df_concacaf_2022_classif.to_csv(f'{silver_path}2022/df_concacaf_2022_classif.csv', sep=';', index=False)
df_uefa_2022_classif.to_csv(f'{silver_path}2022/df_uefa_2022_classif.csv', sep=';', index=False)
df_caf_2022_classif.to_csv(f'{silver_path}2022/df_caf_2022_classif.csv', sep=';', index=False)
df_afc_2022_classif.to_csv(f'{silver_path}2022/df_afc_2022_classif.csv', sep=';', index=False)

df_conmebol_2018_classif.to_csv(f'{silver_path}2018/df_conmebol_2018_classif.csv', sep=';', index=False)
df_concacaf_2018_classif.to_csv(f'{silver_path}2018/df_concacaf_2018_classif.csv', sep=';', index=False)
df_uefa_2018_classif.to_csv(f'{silver_path}2018/df_uefa_2018_classif.csv', sep=';', index=False)
df_caf_2018_classif.to_csv(f'{silver_path}2018/df_caf_2018_classif.csv', sep=';', index=False)
df_afc_2018_classif.to_csv(f'{silver_path}2018/df_afc_2018_classif.csv', sep=';', index=False)

df_conmebol_2014_classif.to_csv(f'{silver_path}2014/df_conmebol_2014_classif.csv', sep=';', index=False)
df_concacaf_2014_classif.to_csv(f'{silver_path}2014/df_concacaf_2014_classif.csv', sep=';', index=False)
df_uefa_2014_classif.to_csv(f'{silver_path}2014/df_uefa_2014_classif.csv', sep=';', index=False)
df_caf_2014_classif.to_csv(f'{silver_path}2014/df_caf_2014_classif.csv', sep=';', index=False)
df_afc_2014_classif.to_csv(f'{silver_path}2014/df_afc_2014_classif.csv', sep=';', index=False)

df_conmebol_2010_classif.to_csv(f'{silver_path}2010/df_conmebol_2010_classif.csv', sep=';', index=False)
df_concacaf_2010_classif.to_csv(f'{silver_path}2010/df_concacaf_2010_classif.csv', sep=';', index=False)
df_uefa_2010_classif.to_csv(f'{silver_path}2010/df_uefa_2010_classif.csv', sep=';', index=False)
df_caf_2010_classif.to_csv(f'{silver_path}2010/df_caf_2010_classif.csv', sep=';', index=False)
df_afc_2010_classif.to_csv(f'{silver_path}2010/df_afc_2010_classif.csv', sep=';', index=False)
df_ofc_2010_classif.to_csv(f'{silver_path}2010/df_ofc_2010_classif.csv', sep=';', index=False)

df_conmebol_2006_classif.to_csv(f'{silver_path}2006/df_conmebol_2006_classif.csv', sep=';', index=False)
df_concacaf_2006_classif.to_csv(f'{silver_path}2006/df_concacaf_2006_classif.csv', sep=';', index=False)
df_uefa_2006_classif.to_csv(f'{silver_path}2006/df_uefa_2006_classif.csv', sep=';', index=False)
df_caf_2006_classif.to_csv(f'{silver_path}2006/df_caf_2006_classif.csv', sep=';', index=False)
df_afc_2006_classif.to_csv(f'{silver_path}2006/df_afc_2006_classif.csv', sep=';', index=False)
df_ofc_2006_classif.to_csv(f'{silver_path}2006/df_ofc_2006_classif.csv', sep=';', index=False)


# salvando os dataframes das selecoes desclassificadas
df_conmebol_2022_desclassif.to_csv(f'{silver_path}2022/df_conmebol_2022_desclassif.csv', sep=';', index=False)
df_concacaf_2022_desclassif.to_csv(f'{silver_path}2022/df_concacaf_2022_desclassif.csv', sep=';', index=False)
df_uefa_2022_desclassif.to_csv(f'{silver_path}2022/df_uefa_2022_desclassif.csv', sep=';', index=False)
df_caf_2022_desclassif.to_csv(f'{silver_path}2022/df_caf_2022_desclassif.csv', sep=';', index=False)
df_afc_2022_desclassif.to_csv(f'{silver_path}2022/df_afc_2022_desclassif.csv', sep=';', index=False)

df_conmebol_2018_desclassif.to_csv(f'{silver_path}2018/df_conmebol_2018_desclassif.csv', sep=';', index=False)
df_concacaf_2018_desclassif.to_csv(f'{silver_path}2018/df_concacaf_2018_desclassif.csv', sep=';', index=False)
df_uefa_2018_desclassif.to_csv(f'{silver_path}2018/df_uefa_2018_desclassif.csv', sep=';', index=False)
df_caf_2018_desclassif.to_csv(f'{silver_path}2018/df_caf_2018_desclassif.csv', sep=';', index=False)
df_afc_2018_desclassif.to_csv(f'{silver_path}2018/df_afc_2018_desclassif.csv', sep=';', index=False)

df_conmebol_2014_desclassif.to_csv(f'{silver_path}2014/df_conmebol_2014_desclassif.csv', sep=';', index=False)
df_concacaf_2014_desclassif.to_csv(f'{silver_path}2014/df_concacaf_2014_desclassif.csv', sep=';', index=False)
df_uefa_2014_desclassif.to_csv(f'{silver_path}2014/df_uefa_2014_desclassif.csv', sep=';', index=False)
df_caf_2014_desclassif.to_csv(f'{silver_path}2014/df_caf_2014_desclassif.csv', sep=';', index=False)
df_afc_2014_desclassif.to_csv(f'{silver_path}2014/df_afc_2014_desclassif.csv', sep=';', index=False)

df_conmebol_2010_desclassif.to_csv(f'{silver_path}2010/df_conmebol_2010_desclassif.csv', sep=';', index=False)
df_concacaf_2010_desclassif.to_csv(f'{silver_path}2010/df_concacaf_2010_desclassif.csv', sep=';', index=False)
df_uefa_2010_desclassif.to_csv(f'{silver_path}2010/df_uefa_2010_desclassif.csv', sep=';', index=False)
df_caf_2010_desclassif.to_csv(f'{silver_path}2010/df_caf_2010_desclassif.csv', sep=';', index=False)
df_afc_2010_desclassif.to_csv(f'{silver_path}2010/df_afc_2010_desclassif.csv', sep=';', index=False)
df_ofc_2010_desclassif.to_csv(f'{silver_path}2010/df_ofc_2010_desclassif.csv', sep=';', index=False)

df_conmebol_2006_desclassif.to_csv(f'{silver_path}2006/df_conmebol_2006_desclassif.csv', sep=';', index=False)
df_concacaf_2006_desclassif.to_csv(f'{silver_path}2006/df_concacaf_2006_desclassif.csv', sep=';', index=False)
df_uefa_2006_desclassif.to_csv(f'{silver_path}2006/df_uefa_2006_desclassif.csv', sep=';', index=False)
df_caf_2006_desclassif.to_csv(f'{silver_path}2006/df_caf_2006_desclassif.csv', sep=';', index=False)
df_afc_2006_desclassif.to_csv(f'{silver_path}2006/df_afc_2006_desclassif.csv', sep=';', index=False)
df_ofc_2006_desclassif.to_csv(f'{silver_path}2006/df_ofc_2006_desclassif.csv', sep=';', index=False)
