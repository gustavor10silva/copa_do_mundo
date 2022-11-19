# importando as bibliotecas
import pandas as pd


# extraindo o csv da url
url = 'https://raw.githubusercontent.com/jfjelstul/worldcup/master/data-csv/matches.csv'
df_jogos = pd.read_csv(url)


# ingerindo os dataframes na camada bronze
lista_copas = [
    ['2018', 'WC-2018'],
    ['2014', 'WC-2014'],
    ['2010', 'WC-2010'],
    ['2006', 'WC-2006']
    ]

for copa in lista_copas:
    ano = copa[0]
    tournament_id = copa[1]

    df_jogos_filtro = df_jogos[df_jogos['tournament_id'] == tournament_id]
    df_jogos_filtro.to_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/{ano}/df_jogos.csv', sep=';', index=False)