import numpy as np
import pandas as pd


df_otimizacao = pd.read_csv('us8/resultado_otimizador-formato-antigo.csv', sep=';', decimal= ',')
display(df_otimizacao.apply(lambda x: x.astype(str).str.replace('.', ',')))


df_otimizacao.loc[(df_otimizacao['TAG'].str.contains('GQ'))].head(55)


