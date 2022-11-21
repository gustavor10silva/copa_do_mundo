# importando bibliotecas
import pandas as pd


# tratando as bases da camada silver e ingerindo na camada gold
anos = ['2006', '2010', '2014', '2018', '2022']

for ano in anos:
    df_stats = pd.read_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/silver/{ano}/df_stats_{ano}.csv', sep=';')
    selecoes = df_stats['selecao'].value_counts().index
    df_stats_trat = pd.DataFrame(selecoes, columns=['selecao'])
    
    for funcao in ['ataque', 'defesa']:

        for col in ['idade', 'score']:
            col_min = []
            col_max = []
            col_mean = []

            for selecao in selecoes:
                col_min.append(round(df_stats[(df_stats['selecao'] == selecao) & (df_stats['funcao'] == funcao)][col].min(), 2))
                col_max.append(round(df_stats[(df_stats['selecao'] == selecao) & (df_stats['funcao'] == funcao)][col].max(), 2))
                col_mean.append(round(df_stats[(df_stats['selecao'] == selecao) & (df_stats['funcao'] == funcao)][col].mean(), 2))

            df_stats_trat[f'{funcao}_{col}_min'] = col_min
            df_stats_trat[f'{funcao}_{col}_max'] = col_max
            df_stats_trat[f'{funcao}_{col}_mean'] = col_mean

    if ano == '2006':
        linha_append = [['arabia_saudita'] + [None] * (df_stats_trat.shape[1] - 1)]
        df_append = pd.DataFrame(linha_append, columns=list(df_stats_trat.columns))
        df_stats_trat = pd.concat([df_stats_trat, df_append], ignore_index=True)

    for col in df_stats_trat.columns:
        try:
            df_stats_trat[col].fillna(round(df_stats_trat[col].mean(),2), inplace=True)
        except:
            pass
        
    df_stats_trat.to_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/gold/{ano}/df_stats_{ano}.csv', sep=';', index=False)