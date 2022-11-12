# TRATAMENTO DE DADOS - BRONZE TO SILVER
# - padronizacao dos nomes das colunas
# - tipagem de variaveis
# - separacao de dataframes por escopo (classif e desclassif)
# - ingestao na camada silver


# importando bibliotecas
import pandas as pd


# importando os dataframes da camada bronze
df_conmebol = pd.read_csv('bronze/2022/df_conmebol.csv', sep=';')
df_concacaf = pd.read_csv('bronze/2022/df_concacaf.csv', sep=';')
df_uefa = pd.read_csv('bronze/2022/df_uefa.csv', sep=';')
df_caf = pd.read_csv('bronze/2022/df_caf.csv', sep=';')
df_afc = pd.read_csv('bronze/2022/df_afc.csv', sep=';')


# padronizacao dos nomes das colunas
df_conmebol['conf_cont'] = 'conmebol'
df_conmebol.rename(columns={
    'Pos':'pos_elim',
    'Equipe':'selecao',
    'Pts':'pts',
    'J':'j',
    'V':'v',
    'E':'e',
    'D':'d',
    'GP':'gp',
    'GC':'gc',
    'SG':'sg',
    'Classificação':'classif'
}, inplace=True)

df_concacaf['conf_cont'] = 'concacaf'
df_concacaf.rename(columns={
    'Pos.':'pos_elim',
    'vdeSeleção':'selecao',
    'Pts':'pts',
    'J':'j',
    'V':'v',
    'E':'e',
    'D':'d',
    'GP':'gp',
    'GC':'gc',
    'SG':'sg',
    'Classificação':'classif'
}, inplace=True)

df_uefa['conf_cont'] = 'uefa'
df_uefa.rename(columns={
    'Pos':'pos_elim',
    'Equipe':'selecao',
    'Pts':'pts',
    'J':'j',
    'V':'v',
    'E':'e',
    'D':'d',
    'GP':'gp',
    'GC':'gc',
    'SG':'sg',
    'Classificação':'classif'
}, inplace=True)

df_caf['conf_cont'] = 'caf'
df_caf['classif'] = None
df_caf.rename(columns={
    'Pos.':'pos_elim',
    'vdeSeleção':'selecao',
    'Pts':'pts',
    'J':'j',
    'V':'v',
    'E':'e',
    'D':'d',
    'GP':'gp',
    'GC':'gc',
    'SG':'sg'
}, inplace=True)

df_afc['conf_cont'] = 'afc'
df_afc.rename(columns={
    'Pos':'pos_elim',
    'Equipe':'selecao',
    'Pts':'pts',
    'J':'j',
    'V':'v',
    'E':'e',
    'D':'d',
    'GP':'gp',
    'GC':'gc',
    'SG':'sg',
    'Classificação':'classif'
}, inplace=True)


# tipagem de variaveis
df_conmebol['sg'] = df_conmebol['sg'].replace('−', '-', regex=True).astype(int)
df_concacaf['sg'] = df_concacaf['sg'].replace('–', '-', regex=True).astype(int)
df_uefa['sg'] = df_uefa['sg'].replace('−', '-', regex=True).astype(int)
df_caf['sg'] = df_caf['sg'].replace('–', '-', regex=True).astype(int)
df_afc['sg'] = df_afc['sg'].replace('−', '-', regex=True).astype(int)


# separacao de dataframes por escopo (classif e desclassif)
selecoes_classificadas = [
    'Qatar', 'Equador', 'Senegal', 'Países Baixos',
    'Inglaterra', 'Irã', 'Estados Unidos', 'País de Gales',
    'Argentina', 'Arábia Saudita', 'México', 'Polónia',
    'França', 'Austrália', 'Dinamarca', 'Tunísia',
    'Espanha', 'Costa Rica', 'Alemanha', 'Japão',
    'Bélgica', 'Canadá', 'Marrocos', 'Croácia',
    'Brasil', 'Sérvia', 'Suíça', 'Camarões',
    'Portugal', 'Gana', 'Uruguai', 'Coreia do Sul']

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