# importando bibliotecas
import pandas as pd
from unidecode import unidecode


# dicionario de padronizacao dos nomes das selecoes
dict_replace = {
    'Germany' : 'alemanha',
    'Australia' : 'australia',
    'South Korea' : 'coreia_do_sul',
    'Switzerland' : 'suica',
    'Mexico' : 'mexico',
    'France' : 'franca',
    'Japan' : 'japao',
    'Spain' : 'espanha',
    'Argentina' : 'argentina',
    'Portugal' : 'portugal',
    'England' : 'inglaterra',
    'Brazil' : 'brasil',
    'Nigeria' : 'nigeria',
    'Uruguay' : 'uruguai',
    'Croatia' : 'croacia',
    'Italy' : 'italia',
    'Ivory Coast' : 'costa_do_marfim',
    'Iran' : 'ira',
    'Costa Rica' : 'costa_rica',
    'Ghana' : 'gana',
    'United States' : 'estados_unidos',
    'Netherlands' : 'holanda',
    'Greece' : 'grecia',
    'Chile' : 'chile',
    'Saudi Arabia' : 'arabia_saudita',
    'Algeria' : 'argelia',
    'Serbia and Montenegro' : 'servia',
    'Serbia' : 'servia',
    'Colombia' : 'colombia',
    'Honduras' : 'honduras',
    'Poland' : 'polonia',
    'Belgium' : 'belgica',
    'Cameroon' : 'camaroes',
    'Russia' : 'russia',
    'Denmark' : 'dinamarca',
    'Sweden' : 'suecia',
    'Ecuador' : 'equador',
    'Paraguay' : 'paraguai',
    'Tunisia' : 'tunisia',
    'Bosnia and Herzegovina' : 'bosnia_e_herzegovina',
    'North Korea' : 'coreia_do_norte',
    'Senegal' : 'senegal',
    'Peru' : 'peru',
    'Egypt' : 'egito',
    'Morocco' : 'marrocos',
    'Iceland' : 'islandia',
    'Angola' : 'angola',
    'New Zealand' : 'nova_zelandia',
    'Slovenia' : 'eslovenia',
    'Slovakia' : 'eslovaquia',
    'South Africa' : 'africa_do_sul',
    'Togo' : 'togo',
    'Trinidad and Tobago' : 'trindade_e_tobago',
    'Ukraine' : 'ucrania',
    'Czech Republic' : 'republica_tcheca',
    'Panama' : 'panama'
}


# carregando os dataframes da camada bronze, tratando e ingerindo na camada silver
anos = ['2006', '2010', '2014', '2018']
for ano in anos:
    df_jogos = pd.read_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/bronze/{ano}/df_jogos.csv', sep=';')
    df_jogos = df_jogos[['home_team_name', 'away_team_name', 'home_team_win']]
    df_jogos['ano'] = ano

    for col in ['home_team_name', 'away_team_name']:
        for nome_velho, nome_novo in dict_replace.items():
            df_jogos[col] = df_jogos[col].str.replace(nome_velho, nome_novo)

    df_jogos.to_csv(f'C:/Users/Rustabo/Projetos/copa_do_mundo/silver/{ano}/df_jogos_{ano}.csv', sep=';', index=False)