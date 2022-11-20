# importando bibliotecas
import pandas as pd


# criando o dataframe final vazio
cols = [
    'home_team_name', 'away_team_name', 'home_team_win', 'ano', 'home_ataque_idade_min', 'home_ataque_idade_max', 'home_ataque_idade_mean',
    'home_ataque_score_min', 'home_ataque_score_max', 'home_ataque_score_mean', 'home_defesa_idade_min', 'home_defesa_idade_max',
    'home_defesa_idade_mean', 'home_defesa_score_min', 'home_defesa_score_max', 'home_defesa_score_mean', 'home_pos_elim', 'home_pts',
    'home_j', 'home_v', 'home_e', 'home_d', 'home_gp', 'home_gc', 'home_sg', 'home_conf_cont', 'home_desclassif_qtd', 'home_desclassif_pts_min',
    'home_desclassif_pts_max', 'home_desclassif_pts_mean', 'home_desclassif_v_min', 'home_desclassif_v_max', 'home_desclassif_v_mean',
    'home_desclassif_e_min', 'home_desclassif_e_max', 'home_desclassif_e_mean', 'home_desclassif_d_min', 'home_desclassif_d_max',
    'home_desclassif_d_mean', 'home_desclassif_gp_min', 'home_desclassif_gp_max', 'home_desclassif_gp_mean', 'home_desclassif_gc_min',
    'home_desclassif_gc_max', 'home_desclassif_gc_mean', 'home_desclassif_sg_min', 'home_desclassif_sg_max', 'home_desclassif_sg_mean',
    'home_aprov', 'home_perc_v', 'home_repescagem', 'home_gp_por_jogo', 'home_gc_por_jogo', 'home_sg_por_jogo', 'home_sede', 'away_ataque_idade_min',
    'away_ataque_idade_max', 'away_ataque_idade_mean', 'away_ataque_score_min', 'away_ataque_score_max', 'away_ataque_score_mean',
    'away_defesa_idade_min', 'away_defesa_idade_max', 'away_defesa_idade_mean', 'away_defesa_score_min', 'away_defesa_score_max',
    'away_defesa_score_mean', 'away_pos_elim', 'away_pts', 'away_j', 'away_v', 'away_e', 'away_d', 'away_gp', 'away_gc', 'away_sg', 'away_conf_cont',
    'away_desclassif_qtd', 'away_desclassif_pts_min', 'away_desclassif_pts_max', 'away_desclassif_pts_mean', 'away_desclassif_v_min',
    'away_desclassif_v_max', 'away_desclassif_v_mean', 'away_desclassif_e_min', 'away_desclassif_e_max', 'away_desclassif_e_mean',
    'away_desclassif_d_min', 'away_desclassif_d_max', 'away_desclassif_d_mean', 'away_desclassif_gp_min', 'away_desclassif_gp_max',
    'away_desclassif_gp_mean', 'away_desclassif_gc_min', 'away_desclassif_gc_max', 'away_desclassif_gc_mean', 'away_desclassif_sg_min',
    'away_desclassif_sg_max', 'away_desclassif_sg_mean', 'away_aprov', 'away_perc_v', 'away_repescagem', 'away_gp_por_jogo', 'away_gc_por_jogo',
    'away_sg_por_jogo', 'away_sede'
    ]
df_copas = pd.DataFrame(columns=cols)


# populando os dataframes finais de cada ano e concatenando em um único dataframe
anos = ['2006', '2010', '2014', '2018']

for ano in anos:
    df_jogos = pd.read_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/gold/{ano}/df_jogos_{ano}.csv', sep=';')
    df_stats = pd.read_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/gold/{ano}/df_stats_{ano}.csv', sep=';')
    df_elim = pd.read_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/gold/{ano}/df_classif_{ano}.csv', sep=';')

    for categoria in ['home', 'away']:

        for df in [df_stats, df_elim]:

            for col in df.columns:
                if col != 'selecao':
                    lista_col = []

                    for selecao in df_jogos[f'{categoria}_team_name']:
                        try:
                            lista_col.append(int(df[df['selecao'] == selecao][col]))
                        except:
                            lista_col.append(str(df[df['selecao'] == selecao][col]))
                    
                    df_jogos[f'{categoria}_{col}'] = lista_col
    
    df_copas = pd.concat([df_copas, df_jogos], ignore_index=True)


# salvando o dataframe final
df_copas.to_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/gold/df_copas.csv', sep=';', index=False)