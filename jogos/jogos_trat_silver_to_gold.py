# importando bibliotecas
import pandas as pd


# carregando os dataframes da camada silver e ingerindo na camada gold
anos = ['2006', '2010', '2014', '2018']
for ano in anos:
    df_jogos = pd.read_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/silver/{ano}/df_jogos_{ano}.csv', sep=';')
    df_jogos.to_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/gold/{ano}/df_jogos_{ano}.csv', sep=';', index=False)