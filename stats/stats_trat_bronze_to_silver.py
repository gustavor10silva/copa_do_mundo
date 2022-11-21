# importando bibliotecas
import pandas as pd
import re


# montando vetores com ano, ataque e defesa
anos_infos = [
    ['2006', 'CAV|MC|MD|CF|ME|ADC|AEC|AD|AE', 'ZC|GOL|CAB|VOL|ZD|ZE|ADR|AER|LÍB'],
    ['2010', 'MD|MC|ME|MEI|SA|PD|ATA|PE', 'GOL|LB|ADD|LD|ZAG|LE|ADE|VOL'],
    ['2014', 'MD|MC|ME|MEI|SA|PD|ATA|PE', 'GOL|LB|ADD|LD|ZAG|LE|ADE|VOL'],
    ['2018', 'MD|MC|ME|MEI|SA|PD|ATA|PE', 'GOL|LB|ADD|LD|ZAG|LE|ADE|VOL'],
    ['2022', 'MD|MC|ME|MEI|SA|PD|ATA|PE', 'GOL|LB|ADD|LD|ZAG|LE|ADE|VOL']
    ]


# fazendo o tratamento basico dos dataframes e salvando na camada silver
for i in range(len(anos_infos)):
    ano = anos_infos[i][0]
    ataque = anos_infos[i][1]
    defesa = anos_infos[i][2]

    df_stats = pd.read_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/{ano}/df_stats.csv', sep=';')
    df_stats = df_stats[(df_stats['Nome'].isna() == False) & (df_stats['page'] <= 2)]
    df_stats.rename(columns={'Idade':'idade'}, inplace=True)
    df_stats = df_stats.astype({'GER-POT':int, 'Posições Preferidas':str, 'idade':int})
    df_stats = df_stats.astype({'GER-POT':str})

    df_stats['ger'] = df_stats['GER-POT'].str[:2]
    df_stats['pot'] = df_stats['GER-POT'].str[2:]
    df_stats = df_stats.astype({'ger':int, 'pot':int})
    df_stats['score'] = (df_stats['ger'] + df_stats['pot']) / 2
    df_stats.drop(columns=['GER-POT', 'Nome', 'page', 'ano', 'ger', 'pot'], inplace=True)

    funcao = [
        'ataque' if re.search(ataque, posicao)
        else ('defesa' if re.search(defesa, posicao) else None)
        for posicao in list(df_stats['Posições Preferidas'])
    ]
    df_stats['funcao'] = funcao
    df_stats.drop(columns=['Posições Preferidas'], inplace=True)
    
    df_stats.reset_index(drop=True, inplace=True)

    df_stats.to_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/silver/{ano}/df_stats_{ano}.csv', sep=';', index=False)