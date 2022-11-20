#%%
# importando bibliotecas
import pandas as pd
import sklearn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor

# %%
# carregando o dataframe final
df = pd.read_csv('C:/Users/Rustabo/Projetos/copa_do_mundo/gold/df_copas.csv', sep=';')
df
# %%
