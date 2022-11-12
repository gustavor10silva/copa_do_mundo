#%%
# importando bibliotecas
import selenium
from selenium import webdriver
import pandas as pd
import time


df_classif = pd.read_csv('gold/2022/df_classif_2022.csv', sep=';')
selecoes_classificadas = list(df_classif['selecao'])

driver = webdriver.Chrome('C:\\Users\\Rustabo\\Projetos\\copa_do_mundo\\src\\chromedriver.exe')

#%%
driver.get('https://www.fifaindex.com/pt-br/players/top/fifa22/')
time.sleep(1)
driver.find_element('xpath', '/html/body/main/div/div/div[2]/nav[1]/ol/li[2]/a').click()
time.sleep(1)
driver.find_element('xpath', '/html/body/main/div/div/div[2]/nav[1]/ol/li[2]/div/a[2]').click()
time.sleep(1)

#for selecao in selecoes_classificadas:
driver.find_element('xpath', '//*[@id="id_nationality-tomselected"]').send_keys('Brasil')


#%%

selecoes_classificadas
# %%
