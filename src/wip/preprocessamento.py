import re
import math
import json
import pickle
import numbers
import os
import os.path
import sys
import glob
import itertools
import subprocess
import warnings

import importlib
import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import pytz
import shap
from datetime import datetime, timedelta
from scipy.interpolate import interp1d
from IPython.display import display, clear_output

warnings.filterwarnings('ignore')


testing = False
critical_test = False


# MAGIC %md
# MAGIC # seta janela de dias


#  Usar numero menor caso queira testar codigo.
n_dias = 547 # padrão: 547 dias.
n_dias2 = 730 # padrao: 730 dias


# MAGIC %md
# MAGIC # carrega variaveis e funções
# MAGIC Mapeamento de pastas, arquivos e funções:
# MAGIC 
# MAGIC **modules**
# MAGIC * predictive_module
# MAGIC * model_module
# MAGIC * utils
# MAGIC * sql_module
# MAGIC   * tagPIMStoDataLake
# MAGIC   * getFVarParam
# MAGIC   * getTAGsDelay
# MAGIC 
# MAGIC **files**
# MAGIC * process_tag_map
# MAGIC * datasets_limits
# MAGIC * datasets_confs
# MAGIC * tags
# MAGIC 
# MAGIC **analises**
# MAGIC * analise_database


import .Otimizacao.modules.predictive_module


import ....Utils.model_module


import ....Utils.utils


# MAGIC %md
# MAGIC * sql_module: Executando o notebook sql_module para conectar ao banco de dados, realizar consultas no Data Lake e no banco de dados 


import ....Utils.sql_module


import .Otimizacao.files.process_tag_map


import .Otimizacao.files.datasets_limits


import .Otimizacao.files.datasets_confs


# MAGIC %md
# MAGIC * Executando notebook que contém todas as tags utilizadas para Usina 8 


import files.tags


import ....analises.analise_database


#Definindo o número da usina 
us_sufix = '08'
solver_path = f'us{us_sufix}'.replace('0', '')


# MAGIC %md
# MAGIC # continua


#Definindo o número da usina 
n_usina = 8
# plotDebug = True

#Definindo a faixa de produção da usina 
faixamin = 700
faixamax = 1000
df_faixa = pd.DataFrame({'faixaMin':list(range(faixamin, faixamax, 50)), 'faixaMax':list(range(faixamin + 50, faixamax + 50, 50))})


#Atribuindo a tagpims todas as tags contidas na listagem 
tagpims = tagpims + tagOTM + taglab + tag_calc
old_len = len(tagpims)
tagpims = list(set(tagpims))

#Verificação de de duplicidade 
if old_len != len(tagpims): log.info('there are duplicates')


#Atribuindo a tagdl, o formato do datalake 
tagdl = sqlModule(dbutils).tagPIMStoDataLake(tagpims)
#Dicionário com o formato do Data Lake e do Pims 
taglist = pd.DataFrame({'tagdl': tagdl, 'tagpims': tagpims})


# dicTags = dict(zip(taglist.tagdl, taglist.tagpims))


# MAGIC %md
# MAGIC # define tini e tfim


#Definindo a data inicial e data final de busca 
tz = pytz.timezone('America/Sao_Paulo')
selected_time = datetime(2022, 7, 22, 0, 0, 0, tzinfo=tz)
if testing: 
    tfim = selected_time
else: 
    tfim = datetime.now(tz)
tini = tfim - timedelta(days=n_dias)  # dois anos

# if testing: tfim = selected_time
# else: tfim = datetime.datetime.now(tz)
tini = tini.replace(microsecond=0, second=0, minute=0)
tfim = tfim.replace(microsecond=0, second=0, minute=0)

tini_str = tini.strftime('%Y-%m-%d %H:%M:%S')
log.info(f'{tini}, "\n",      {tfim}')


# MAGIC %md
# MAGIC # getFVarParam e getTAGsDelay


#Horário que começa a rodar o comando 
log.info(f'command started at: {datetime.now(tz)}')

# pode dar erro no pyodbc
df_sql = sqlModule(dbutils).getFVarParam(n_usina, tini_str, tfim, taglist, param="US8")

#Verifica se possui duplicidade 
df_sql_grouped = df_sql.groupby(['variavel']).count().reset_index()
df_sql_grouped[df_sql_grouped['variavel'].str.startswith('GRAN_OCS_10-16')]

#Apaga os registros duplicados encontrados 
df_sql = df_sql.drop_duplicates(subset=['data', 'variavel'], keep="first")
print(df_sql.shape)


#Agrupando data, variavel para verificar se possui contador maior que 1 
df_sql.groupby(['data', 'variavel']).count()


#Realiza pivot (tabelas dinâmicas) que permitem resumir a base de dados e analisar pelas colunas data, variavel e valor
df_sql = df_sql.pivot(index = 'data', columns = 'variavel', values = 'valor')


# o que vem do sql
print(len(df_sql.columns))
print(len(tagpims))	
# o que deveria vir do sql
print(len(tagpims))

# o que deveria vir que não veio
print(len(set(tagpims) - set(df_sql.columns)))

falta = list(set(tagpims) - set(df_sql.columns))

print(list(set(falta) - set(tag_calc)))


# MAGIC %md
# MAGIC ### salva dados para checagem de zeros e nulos
# MAGIC descomentar para análise


# teste = get_nulls_zeros(df_sql)


# df_sql_2021 = df_sql[df_sql.index < datetime(2022, 1,1)]
# df_sql_2022 = df_sql[df_sql.index >= datetime(2022, 1,1)]

# teste_2021 = get_nulls_zeros(df_sql_2021)
# teste_2022 = get_nulls_zeros(df_sql_2022)


# df_sql.isna().sum().reset_index().sort_values(by = [0], ascending=False).head(10)


# df_sql.index.min(), df_sql.index.max()


# MAGIC %md
# MAGIC ### fim do teste de nulos


# log.info(f'command started at: {datetime.now()}')

#Verifica se as tags possuem algum delay 
df_sql = sqlModule(dbutils, spark).getTAGsDelay(df_sql, tini, tfim, tagpims, taglist)
df_sql_raw = df_sql.copy()
df_sql = df_sql.copy()


df_sql = df_sql_raw.copy()


# MAGIC %md
# MAGIC # add restriçoes


# df_sql = df_sql[df_sql.index <= '2020-11-01 23:00:00']
df_sql = df_sql[df_sql['PROD_PQ_Y@08US'] > 0]


# add gabi
# Pressão de vácuo
for t in df_sql.loc[:, ['PRES1_I@08FI-FL-827I-01', 'PRES1_I@08FI-FL-827I-02', 'PRES1_I@08FI-FL-827I-03', 'PRES1_I@08FI-FL-827I-04', 'PRES1_I@08FI-FL-827I-05R', 'PRES1_I@08FI-FL-827I-06',
           'PRES1_I@08FI-FL-827I-07', 'PRES1_I@08FI-FL-827I-08', 'PRES1_I@08FI-FL-827I-09', 'PRES1_I@08FI-FL-827I-10R']]:
    df_sql.loc[df_sql[t] > -.8, t] = np.nan


df_sql = df_sql.drop_duplicates(keep=False)
df_sql = df_sql.replace([-np.inf, np.inf], np.nan)


for t in df_sql.loc[:, [x for x in df_sql.columns if 'ROTA1_I@08FI-FL-827I-' in x]]:
    df_sql.loc[((df_sql[t] < 0.8) | (df_sql[t] > 1)), t] = np.nan
    
df_sql = df_sql.replace([np.inf, -np.inf], np.nan).fillna(method='bfill').fillna(method='ffill').fillna(method='bfill').fillna(0)


# MAGIC %md
# MAGIC #calculadas
# MAGIC * Tags que precisam realizar cálculo para atender as necessidades da usina


# 1
# df_sql['CONS ESP VENT TOTAL - US8'] = df_sql['CONS1_Y@08QU-PF-852I-01M1'] + df_sql['CONS1_Y@08QU-PF-852I-02M1'] + df_sql['CONS1_Y@08QU-PF-852I-03M1'] + df_sql['CONS1_Y@08QU-PF-852I-04M1'] + df_sql['CONS1_Y@08QU-PF-852I-05M1'] + df_sql['CONS1_Y@08QU-PF-852I-06M1'] + df_sql['CONS1_Y@08QU-PF-852I-07M1'] + df_sql['CONS1_Y@08QU-PF-852I-08M1']
df_sql['CONS ESP VENT TOTAL - US8'] = df_sql['CONS1_Y@08QU-VENT']

# 2
# p = df_sql['PESO1_I@08MO-BW-821I-03M1'] > 0.1
# df_sql['Consumo de Energia (base minério úmido) kWh/ton 3'] = (df_sql['POTE1_I@08MO-MO-821I-03M1'][p]/df_sql['PESO1_I@08MO-BW-821I-03M1'][p])*1000

# 3
p = df_sql['PROD_PQ_Y@08US'] > 0
df_sql['PERMEABILIDADE CV27 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-27'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 49 - é pre requisito para outra coluna
df_sql['Vel Periferica R Fixo [m/s]']= (math.pi*df_sql['ROTA1_I@08PR-RP-822I-01M1']*(2/57.07))/60

# 4
p=df_sql['Vel Periferica R Fixo [m/s]']>0
df_sql['PRODUTI MDOT PRENSA - US8'] = df_sql['PESO1_I@08PR-BW-822I-01M1'][p]/(2*1.5*df_sql['Vel Periferica R Fixo [m/s]'][p])

# 5
# df_sql['DESV MEDIO ALT CAMADA'] = mad([df_sql['NIVE1_C@08QU-FR-851I-01M1'], df_sql['NIVE2_I@08QU-FR-851I-01M1'], df_sql['NIVE3_I@08QU-FR-851I-01M1'], df_sql['NIVE4_I@08QU-FR-851I-01M1'], df_sql['NIVE5_I@08QU-FR-851I-01M1'], df_sql['NIVE6_I@08QU-FR-851I-01M1']], axis=0)
df_sql['DESV MEDIO ALT CAMADA'] = df_sql[['NIVE1_C@08QU-FR-851I-01M1', 'NIVE2_I@08QU-FR-851I-01M1', 'NIVE3_I@08QU-FR-851I-01M1', 'NIVE4_I@08QU-FR-851I-01M1', 'NIVE5_I@08QU-FR-851I-01M1', 'NIVE6_I@08QU-FR-851I-01M1']].std(axis=1)

# 6
p = df_sql['PROD_PQ_Y@08US'] > 0
df_sql['PERMEABILIDADE CV18 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-18'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 7
df_sql['PV TEMP GQ3-16-MED - US8'] = np.mean([df_sql['TEMP1_I@08QU-QU-855I-GQ03'], df_sql['TEMP1_I@08QU-QU-855I-GQ04'], df_sql['TEMP1_I@08QU-QU-855I-GQ05'], df_sql['TEMP1_I@08QU-QU-855I-GQ06'], df_sql['TEMP1_I@08QU-QU-855I-GQ06'], df_sql['TEMP1_I@08QU-QU-855I-GQ07'], df_sql['TEMP1_I@08QU-QU-855I-GQ08'], df_sql['TEMP1_I@08QU-QU-855I-GQ09'], df_sql['TEMP1_I@08QU-QU-855I-GQ10'], df_sql['TEMP1_I@08QU-QU-855I-GQ11'], df_sql['TEMP1_I@08QU-QU-855I-GQ12'], df_sql['TEMP1_I@08QU-QU-855I-GQ13'], df_sql['TEMP1_I@08QU-QU-855I-GQ14'], df_sql['TEMP1_I@08QU-QU-855I-GQ15'], df_sql['TEMP1_I@08QU-QU-855I-GQ16']],  axis=0)

# 8
df_sql['PROD FILTR - US8'] = df_sql['PESO1_I@08FI-TR-827I-01M1'] + df_sql['PESO1_I@08FI-TR-827I-02M1']

# 9
p = df_sql['PROD_PQ_Y@08US'] > 0
df_sql['PERMEABILIDADE CV19 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-19'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 57 (pré-requisito para o #10)
df_sql['Nº FILTR FUNC - US8'] = df_sql['FUNC1_D@08FI-FL-827I-01'] + df_sql['FUNC1_D@08FI-FL-827I-02'] + df_sql['FUNC1_D@08FI-FL-827I-03'] + df_sql['FUNC1_D@08FI-FL-827I-04'] + df_sql['FUNC1_D@08FI-FL-827I-05R'] + df_sql['FUNC1_D@08FI-FL-827I-06'] + df_sql['FUNC1_D@08FI-FL-827I-07'] + df_sql['FUNC1_D@08FI-FL-827I-08'] + df_sql['FUNC1_D@08FI-FL-827I-09'] + df_sql['FUNC1_D@08FI-FL-827I-10R']

df_sql['Nº BV FUNC - US8'] = df_sql['FUNC1_D@08FI-BV-827I-01M1'] + df_sql['FUNC1_D@08FI-BV-827I-02M1'] + df_sql['FUNC1_D@08FI-BV-827I-03M1'] + df_sql['FUNC1_D@08FI-BV-827I-04M1'] + df_sql['FUNC1_D@08FI-BV-827I-05RM1'] + df_sql['FUNC1_D@08FI-BV-827I-06M1'] + df_sql['FUNC1_D@08FI-BV-827I-07M1'] + df_sql['FUNC1_D@08FI-BV-827I-08M1'] + df_sql['FUNC1_D@08FI-BV-827I-09M1'] + df_sql['FUNC1_D@08FI-BV-827I-10RM1']

# 10
# df_sql['Relação bombas x filtros'] = df_sql['Nº BV FUNC - US8']/df_sql['Nº FILTR FUNC - US8']

# 11
p = df_sql['PROD_PQ_Y@08US'] > 0
df_sql['PERMEABILIDADE CV3A - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-03A'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 12
df_sql['Vel Periferica R Móvel [m/s]'] = (math.pi*df_sql['ROTA1_I@08PR-RP-822I-01M2']*(2/57.07))/60

# 13
df_sql['Paralelismo'] = np.abs(df_sql['POSI1_I@08PR-RP-822I-01'] - df_sql['POSI2_I@08PR-RP-822I-01'])

# 14
p = df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV15 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-15'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 15
p = df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV3B - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-03B'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 16
p = df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV2 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-02'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 17
p = df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV12 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-12'] / df_sql['PROD_PQ_Y@08US'][p]

# 18
df_sql['MAIOR - MENOR ALT CAMADA'] = np.max([df_sql['NIVE1_C@08QU-FR-851I-01M1'], df_sql['NIVE2_I@08QU-FR-851I-01M1'], df_sql['NIVE3_I@08QU-FR-851I-01M1'], df_sql['NIVE4_I@08QU-FR-851I-01M1'], df_sql['NIVE5_I@08QU-FR-851I-01M1'], df_sql['NIVE6_I@08QU-FR-851I-01M1']], axis=0) - np.min([df_sql['NIVE1_C@08QU-FR-851I-01M1'], df_sql['NIVE2_I@08QU-FR-851I-01M1'], df_sql['NIVE3_I@08QU-FR-851I-01M1'], df_sql['NIVE4_I@08QU-FR-851I-01M1'], df_sql['NIVE5_I@08QU-FR-851I-01M1'], df_sql['NIVE6_I@08QU-FR-851I-01M1']], axis=0)

# 19
p = df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV5 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-05'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 20
# df_sql['PRES MED VACUO - US8'] = np.mean([df_sql['PRES1_I@08FI-BV-827I'], df_sql['PRES1_I@08FI-BV-827I-01'], df_sql['PRES1_I@08FI-BV-827I-02'], df_sql['PRES1_I@08FI-BV-827I-03'], df_sql['PRES1_I@08FI-BV-827I-04'], df_sql['PRES1_I@08FI-BV-827I-05R'], df_sql['PRES1_I@08FI-BV-827I-06'], df_sql['PRES1_I@08FI-BV-827I-07'], df_sql['PRES1_I@08FI-BV-827I-08'], df_sql['PRES1_I@08FI-BV-827I-09'], df_sql['PRES1_I@08FI-BV-827I-10R']], axis=0)

# 21
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV9 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-09'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 22
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV7 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-07'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 23
df_sql['GANHO PRENSA - US8'] = df_sql['SUP_SE_PP_L@08PR'] - df_sql['SUP_SE_PR_L@08FI']

# 24
# df_sql['SP TEMP GQ-MED GQ3-16 - US8'] = np.mean([df_sql['TEMP1_S@08QU-QU-855I-GQ03'], df_sql['TEMP1_S@08QU-QU-855I-GQ04'], df_sql['TEMP1_S@08QU-QU-855I-GQ05'], df_sql['TEMP1_S@08QU-QU-855I-GQ06'], df_sql['TEMP1_S@08QU-QU-855I-GQ07'], df_sql['TEMP1_S@08QU-QU-855I-GQ08'], df_sql['TEMP1_S@08QU-QU-855I-GQ09'], df_sql['TEMP1_S@08QU-QU-855I-GQ10'], df_sql['TEMP1_S@08QU-QU-855I-GQ11'], df_sql['TEMP1_S@08QU-QU-855I-GQ12'], df_sql['TEMP1_S@08QU-QU-855I-GQ13'], df_sql['TEMP1_S@08QU-QU-855I-GQ14'], df_sql['TEMP1_S@08QU-QU-855I-GQ16']],  axis=0)

# 25
df_sql['PERMEABILIDADE CV31 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-31'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 26
# p = df_sql['PESO1_I@08MO-BW-821I-01M1']>0
# df_sql['Desgaste de Corpo Moedor ton/h'] = df_sql['PESO1_I@08MO-BL-821I-01-MO01'][p]/df_sql['PESO1_I@08MO-BW-821I-01M1'][p]

# 27
p1=df_sql['PESO1_I@08PR-BW-822I-01M1']>0
p2=df_sql['PRES3_I@08PR-RP-822I-01']>0
df_sql['Escorregamento'] = 1000*df_sql['ROTA1_I@08PR-RP-822I-01M1'][p1&p2]/df_sql['PESO1_I@08PR-BW-822I-01M1'][p1&p2]/df_sql['PRES3_I@08PR-RP-822I-01'][p1&p2]

# 28
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV8 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-08'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 29
p=df_sql['Vel Periferica R Fixo [m/s]']>0
df_sql['GAP CALC PRENSA - US8'] = (df_sql['PESO1_I@08PR-BW-822I-01M1'][p]/(1.5*3.5*df_sql['Vel Periferica R Fixo [m/s]'][p]))/3.6

# 30
p=df_sql['PESO1_I@08PR-BW-822I-01M1']>0
# df_sql['CONS EE PRENSA - US8'] = (df_sql['POTE1_I@08PR-RP-822I-01M1'][p] + df_sql['POTE1_I@08PR-RP-822I-01M2'][p])/df_sql['PESO1_I@08PR-BW-822I-01M1'][p]
df_sql['CONS EE PRENSA - US8'] = df_sql['CONS1_Y@08PR-RP-822I-01']

# 31
# df_sql['Tempo de mistura 1'] = df_sql['NIVE1_I@08MI-MI-832I-01'] / 100 * 7 / (( df_sql['PESO1_I@08MI-BW-832I-01M1'] + df_sql['PESO1_I@08MI-LW-832I-01M1']) / 1.9)*3600

# 32
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV16 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-16'] / df_sql['PROD_PQ_Y@08US'][p]

# 33
# df_sql['MED - SP ALTURA CAMADA'] = np.mean([df_sql['NIVE1_C@08QU-FR-851I-01M1'], df_sql['NIVE2_I@08QU-FR-851I-01M1'], df_sql['NIVE3_I@08QU-FR-851I-01M1'], df_sql['NIVE4_I@08QU-FR-851I-01M1'], df_sql['NIVE5_I@08QU-FR-851I-01M1'], df_sql['NIVE6_I@08QU-FR-851I-01M1']], axis=0)-df_sql['NIVE1_S@08QU-FR-851I-01M1']

# 34
# df_sql['Tempo de mistura 2'] = df_sql['NIVE1_I@08MI-MI-832I-02'] / 100*7 / ((df_sql['PESO1_I@08MI-BW-832I-02M1'] + df_sql['PESO1_I@08MI-LW-832I-01M1']) / 1.9)*3600

# 35
df_sql['POT TOTAL VENT - US8'] = df_sql['POTE1_I@08QU-PF-852I-01M1'] + df_sql['POTE1_I@08QU-PF-852I-02M1'] + df_sql['POTE1_I@08QU-PF-852I-03M1'] + df_sql['POTE1_I@08QU-PF-852I-04M1'] + df_sql['POTE1_I@08QU-PF-852I-05M1'] + df_sql['POTE1_I@08QU-PF-852I-06M1']

# 56
df_sql['SOMA POTENCIA BV - US8'] = df_sql['POTE1_I@08FI-BV-827I-01M1'] + df_sql['POTE1_I@08FI-BV-827I-02M1'] + df_sql['POTE1_I@08FI-BV-827I-03M1'] + df_sql['POTE1_I@08FI-BV-827I-04M1'] + df_sql['POTE1_I@08FI-BV-827I-05RM1'] + df_sql['POTE1_I@08FI-BV-827I-06M1'] + df_sql['POTE1_I@08FI-BV-827I-07M1'] + df_sql['POTE1_I@08FI-BV-827I-08M1'] + df_sql['POTE1_I@08FI-BV-827I-09M1'] + df_sql['POTE1_I@08FI-BV-827I-10RM1']

# 36
p=df_sql['PROD FILTR - US8']>0
# df_sql['Calculo da Energia da Filtragem'] = df_sql['SOMA POTENCIA BV - US8'][p]/df_sql['PROD FILTR - US8'][p]
df_sql['Calculo da Energia da Filtragem'] = df_sql['CONS1_Y@08FI-BV-827I']

# 37
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV4 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-04'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 38
# p=df_sql['PESO1_I@08MO-BW-821I-01M1']>0.1
# df_sql['Consumo de Energia (base minério úmido) kWh/ton 1'] = (df_sql['POTE1_I@08MO-MO-821I-01M1'][p]/df_sql['PESO1_I@08MO-BW-821I-01M1'][p])*1000
df_sql['Consumo de Energia (base minério úmido) kWh/ton'] = df_sql['CONS1_Y@08MO-MOAG']

# 39
df_sql['RPM MED FILTROS - US8'] = np.mean([df_sql['ROTA1_I@08FI-FL-827I-01M1'],df_sql['ROTA1_I@08FI-FL-827I-02M1'],df_sql['ROTA1_I@08FI-FL-827I-03M1'],df_sql['ROTA1_I@08FI-FL-827I-04M1'],df_sql['ROTA1_I@08FI-FL-827I-05RM1'],df_sql['ROTA1_I@08FI-FL-827I-06M1'],df_sql['ROTA1_I@08FI-FL-827I-07M1'],df_sql['ROTA1_I@08FI-FL-827I-08M1'],df_sql['ROTA1_I@08FI-FL-827I-09M1'],df_sql['ROTA1_I@08FI-FL-827I-10RM1']], axis=0)

# 40
p = df_sql['PROD_PQ_Y@08US'] > 0
df_sql['PERMEABILIDADE CV6 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-06'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 48
df_sql['SOMA FUNC FILTROS'] = df_sql['FUNC1_D@08FI-FL-827I-01'] + df_sql['FUNC1_D@08FI-FL-827I-02'] + df_sql['FUNC1_D@08FI-FL-827I-03'] + df_sql['FUNC1_D@08FI-FL-827I-04'] + df_sql['FUNC1_D@08FI-FL-827I-05R'] + df_sql['FUNC1_D@08FI-FL-827I-06'] + df_sql['FUNC1_D@08FI-FL-827I-07'] + df_sql['FUNC1_D@08FI-FL-827I-08'] + df_sql['FUNC1_D@08FI-FL-827I-09'] + df_sql['FUNC1_D@08FI-FL-827I-10R']

# 59
p=df_sql['SOMA FUNC FILTROS']>0
df_sql['Produtividade Pelotamento Virtual'] = (df_sql['PROD_PC_I@08US'][p]-df_sql['PESO1_I@08PE-TR-851I-03M1'][p])/((7.5/2)*math.pi*df_sql['SOMA FUNC FILTROS'][p])

# 60
p=df_sql['SOMA FUNC FILTROS']>0
df_sql['Produtividade Pel Efetiva'] = (df_sql['PROD_PQ_Y@08US'][p])/(((7.5/2)*math.pi)*df_sql['SOMA FUNC FILTROS'][p])

# 41
df_sql['10 - DIF PRODUTIVI EFETIVA - VIRTUAL - CALC - US8'] = df_sql['Produtividade Pel Efetiva']-df_sql['Produtividade Pelotamento Virtual']

# 42
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV10 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-10'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 43
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV20 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-20'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 44
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV14 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-14'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 45
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV21 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-21'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 46
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV17 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-17'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 47
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV11 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-11'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 50
# df_sql['MV TEMP GQ-MED - US8'] = np.mean([df_sql['TEMP1_O@08QU-QU-855I-GQ01'], df_sql['TEMP1_O@08QU-QU-855I-GQ02'], df_sql['TEMP1_O@08QU-QU-855I-GQ03'],df_sql['TEMP1_O@08QU-QU-855I-GQ04'],df_sql['TEMP1_O@08QU-QU-855I-GQ05'],df_sql['TEMP1_O@08QU-QU-855I-GQ06'],df_sql['TEMP1_O@08QU-QU-855I-GQ07'],df_sql['TEMP1_O@08QU-QU-855I-GQ08'],df_sql['TEMP1_O@08QU-QU-855I-GQ09'],df_sql['TEMP1_O@08QU-QU-855I-GQ10'],df_sql['TEMP1_O@08QU-QU-855I-GQ11'],df_sql['TEMP1_O@08QU-QU-855I-GQ12'],df_sql['TEMP1_O@08QU-QU-855I-GQ13'],df_sql['TEMP1_O@08QU-QU-855I-GQ14'],df_sql['TEMP1_O@08QU-QU-855I-GQ15'],df_sql['TEMP1_O@08QU-QU-855I-GQ16']], axis=0)

# 51
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['CONS ESPEC EE VENT - US8'] = df_sql['POT TOTAL VENT - US8'][p]/df_sql['PROD_PQ_Y@08US'][p]

# 52
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV1 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-01'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 53
df_sql['MAIOR TEMP PQ - US8'] = np.max([df_sql['TEMP1_I@08PN-TR-860I-01'],df_sql['TEMP1_I@08PN-TR-860I-02']], axis=0)

# 54
# p=df_sql['PESO1_I@08MO-BW-821I-02M1']>0.1
# df_sql['Consumo de Energia (base minério úmido) kWh/ton 2'] = (df_sql['POTE1_I@08MO-MO-821I-02M1'][p]/df_sql['PESO1_I@08MO-BW-821I-02M1'][p])*1000

# 55
p=df_sql['PROD_PQ_Y@08US']>0
df_sql['PERMEABILIDADE CV13 - CALC - US8'] = df_sql['PRES1_I@08QU-WB-851I-13'][p] / df_sql['PROD_PQ_Y@08US'][p]

# 58
df_sql['vazao_antracito'] = df_sql['PESO1_I@08MO-BW-813I-03M1'] + df_sql['PESO1_I@08MO-BW-813I-04M1']

# 61
df_sql['%CF. ANT'] = df_sql['QUIM_CFIX_PP_L@08PR'] / df_sql['QUIM_CARVAO_PP_L@08PR']

# 62
p=df_sql['VELO1_C@08QU-FR-851I-01M1']>0
df_sql['=192/VELO'] = 192 / df_sql['VELO1_C@08QU-FR-851I-01M1'][p]

# 63
p=df_sql['FUNC1_D@08QU-FR-851I-01M1']>0
df_sql['=PQ*24/768/FUNC'] = df_sql['PROD_PQ_Y@08US'][p] * 24 / 768 / df_sql['FUNC1_D@08QU-FR-851I-01M1'][p]

# 64 -- Demanda Aileen - SE PP 
df_sql['media vel de disco de pel'] = (((df_sql['ROTA1_I@08PE-BD-840I-01M1']*df_sql['FUNC1_D@08PE-BD-840I-01M1']) + (df_sql['ROTA1_I@08PE-BD-840I-02M1']*df_sql['FUNC1_D@08PE-BD-840I-02M1'])+ (df_sql['ROTA1_I@08PE-BD-840I-03M1']*df_sql['FUNC1_D@08PE-BD-840I-03M1'])+(df_sql['ROTA1_I@08PE-BD-840I-04M1']*df_sql['FUNC1_D@08PE-BD-840I-04M1'])+ (df_sql['ROTA1_I@08PE-BD-840I-05M1']*df_sql['FUNC1_D@08PE-BD-840I-05M1'])+ (df_sql['ROTA1_I@08PE-BD-840I-06M1']*df_sql['FUNC1_D@08PE-BD-840I-06M1'])+(df_sql['ROTA1_I@08PE-BD-840I-07M1']*df_sql['FUNC1_D@08PE-BD-840I-07M1'])+(df_sql['ROTA1_I@08PE-BD-840I-08M1']*df_sql['FUNC1_D@08PE-BD-840I-08M1'])+(df_sql['ROTA1_I@08PE-BD-840I-09M1']*df_sql['FUNC1_D@08PE-BD-840I-09M1'])+(df_sql['ROTA1_I@08PE-BD-840I-10M1']*df_sql['FUNC1_D@08PE-BD-840I-10M1'])+(df_sql['ROTA1_I@08PE-BD-840I-11M1']*df_sql['FUNC1_D@08PE-BD-840I-11M1'])+(df_sql['ROTA1_I@08PE-BD-840I-12M1']*df_sql['FUNC1_D@08PE-BD-840I-12M1']))/(df_sql['FUNC1_D@08PE-BD-840I-01M1'] + df_sql['FUNC1_D@08PE-BD-840I-02M1'] + df_sql['FUNC1_D@08PE-BD-840I-03M1'] + df_sql['FUNC1_D@08PE-BD-840I-04M1']+ df_sql['FUNC1_D@08PE-BD-840I-05M1']+ df_sql['FUNC1_D@08PE-BD-840I-06M1']+ df_sql['FUNC1_D@08PE-BD-840I-07M1']+ df_sql['FUNC1_D@08PE-BD-840I-08M1']+ df_sql['FUNC1_D@08PE-BD-840I-09M1']+ df_sql['FUNC1_D@08PE-BD-840I-10M1']+ df_sql['FUNC1_D@08PE-BD-840I-11M1']+ df_sql['FUNC1_D@08PE-BD-840I-12M1']))

#65 -- Demanda Aileen - SE PP 
df_sql['media gap prensa'] = np.mean([df_sql['POSI1_I@08PR-RP-822I-01'], df_sql['POSI2_I@08PR-RP-822I-01']], axis=0) 
 
#66 -- Demanda Aileen - SE PP 
df_sql['media rolo fixo prensa'] = np.mean([df_sql['POTE1_I@08PR-RP-822I-01M1'], df_sql['POTE1_I@08PR-RP-822I-01M2']],  axis=0)
 
#67 -- Demanda Aileen - SE PP 
#Média e Diferença                                                                                               
df_sql['media oleo prensa'] = np.mean([df_sql['PRES2_I@08PR-RP-822I-01'],df_sql['PRES3_I@08PR-RP-822I-01']],  axis=0)
# df_sql['diferença oleo prensa'] = np.diff(df_sql['PRES2_I@08PR-RP-822I-01'],df_sql['PRES3_I@08PR-RP-822I-01']).(periods=1, axis=1)
#_, df_sql['diferenca oleo prensa'] = (df_sql[['PRES2_I@08PR-RP-822I-01','PRES3_I@08PR-RP-822I-01']]).diff(axis=1)

#68 - Demanda Aileen - SE PR, cfix
df_sql['media balanca moinho'] = np.mean([df_sql['PESO1_I@08MO-BW-821I-01M1'],df_sql['PESO1_I@08MO-BW-821I-02M1'],df_sql['PESO1_I@08MO-BW-821I-03M1']],  axis=0)

#69 - Demanda Aileen - SE PR, SE PP
df_sql['media_vazao_antracito'] = np.mean([df_sql['PESO1_I@08MO-BW-813I-03M1'], df_sql['PESO1_I@08MO-BW-813I-04M1']],  axis=0)

#70 - Demanda Aileen - Modelo Queima, abrasao=compr
df_sql['soma balanca minerio misturador'] = df_sql['PESO1_I@08MI-BW-832I-01M1'] + df_sql['PESO1_I@08MI-BW-832I-02M1']

#70 - Demanda Aileen - Modelo Queima, abrasao=compr
df_sql['soma balanca bentonita misturador'] = df_sql['PESO1_I@08MI-LW-832I-01M1'] + df_sql['PESO1_I@08MI-LW-832I-02M1']

#71 - Demanda Aileen - Modelo Queima, abrasao=compr
df_sql['soma balanca retorno correia'] = df_sql['PESO1_I@08PE-TR-840I-28M1'] + df_sql['PESO1_I@08PE-TR-840I-29M1']
                                                 
#72 - Demanda Aileen - Modelo Queima, abrasao=compr
df_sql['media temp pelotas'] = np.mean([df_sql['TEMP1_I@08PN-TR-860I-01'], df_sql['TEMP1_I@08PN-TR-860I-02']],  axis=0)

# 73 -- media do disco de pelotamento, inserir as tags de funcionamento
#modelos de abrasão, compressão 
df_sql['media disco de pelotamento'] = np.mean([df_sql['ROTA1_I@08PE-BD-840I-01M1'],df_sql['ROTA1_I@08PE-BD-840I-02M1'],df_sql['ROTA1_I@08PE-BD-840I-03M1'],df_sql['ROTA1_I@08PE-BD-840I-04M1'],df_sql['ROTA1_I@08PE-BD-840I-05M1'],df_sql['ROTA1_I@08PE-BD-840I-06M1'],df_sql['ROTA1_I@08PE-BD-840I-07M1'],df_sql['ROTA1_I@08PE-BD-840I-08M1'],df_sql['ROTA1_I@08PE-BD-840I-09M1'],df_sql['ROTA1_I@08PE-BD-840I-10M1'],df_sql['ROTA1_I@08PE-BD-840I-11M1'],df_sql['ROTA1_I@08PE-BD-840I-12M1']],  axis=0)

# # 74
# #Filtro funcionamento do queimador 01 
# p=df_sql['FUNC1_D@08QU-QU-855I-01']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-02']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ01'] = df_sql['TEMP1_I@08QU-QU-855I-GQ01'][p][p1]

# # 75
# #Filtro funcionamento do queimador 02
# p=df_sql['FUNC1_D@08QU-QU-855I-03']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-04']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ02'] = df_sql['TEMP1_I@08QU-QU-855I-GQ02'][p][p1]

# # 76
# #Filtro funcionamento do queimador 03
# p=df_sql['FUNC1_D@08QU-QU-855I-05']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-06']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ03'] = df_sql['TEMP1_I@08QU-QU-855I-GQ03'][p][p1]

# # 77
# #Filtro funcionamento do queimador 04
# p=df_sql['FUNC1_D@08QU-QU-855I-07']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-08']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ04'] = df_sql['TEMP1_I@08QU-QU-855I-GQ04'][p][p1]

# # 78
# #Filtro funcionamento do queimador 05
# p=df_sql['FUNC1_D@08QU-QU-855I-09']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-10']>0
# p2=df_sql['FUNC1_D@08QU-QU-855I-11']>0
# p3=df_sql['FUNC1_D@08QU-QU-855I-12']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ05'] = df_sql['TEMP1_I@08QU-QU-855I-GQ05'][p][p1][p2][p3]

# # 79
# #Filtro funcionamento do queimador 06
# p=df_sql['FUNC1_D@08QU-QU-855I-13']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-14']>0
# p2=df_sql['FUNC1_D@08QU-QU-855I-15']>0
# p3=df_sql['FUNC1_D@08QU-QU-855I-16']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ06'] = df_sql['TEMP1_I@08QU-QU-855I-GQ06'][p][p1][p2][p3]

# # 80
# #Filtro funcionamento do queimador 07
# p=df_sql['FUNC1_D@08QU-QU-855I-17']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-18']>0
# p2=df_sql['FUNC1_D@08QU-QU-855I-19']>0
# p3=df_sql['FUNC1_D@08QU-QU-855I-20']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ07'] = df_sql['TEMP1_I@08QU-QU-855I-GQ07'][p][p1][p2][p3]

# # 81
# #Filtro funcionamento do queimador 08
# p=df_sql['FUNC1_D@08QU-QU-855I-21']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-22']>0
# p2=df_sql['FUNC1_D@08QU-QU-855I-23']>0
# p3=df_sql['FUNC1_D@08QU-QU-855I-24']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ08'] = df_sql['TEMP1_I@08QU-QU-855I-GQ08'][p][p1][p2][p3]

# # 82
# #Filtro funcionamento do queimador 09
# p=df_sql['FUNC1_D@08QU-QU-855I-25']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-26']>0
# p2=df_sql['FUNC1_D@08QU-QU-855I-27']>0
# p3=df_sql['FUNC1_D@08QU-QU-855I-28']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ09'] = df_sql['TEMP1_I@08QU-QU-855I-GQ09'][p][p1][p2][p3]

# # 83
# #Filtro funcionamento do queimador 10
# p=df_sql['FUNC1_D@08QU-QU-855I-29']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-30']>0
# p2=df_sql['FUNC1_D@08QU-QU-855I-31']>0
# p3=df_sql['FUNC1_D@08QU-QU-855I-32']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ10'] = df_sql['TEMP1_I@08QU-QU-855I-GQ10'][p][p1][p2][p3]

# # 84
# #Filtro funcionamento do queimador 11
# p=df_sql['FUNC1_D@08QU-QU-855I-33']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-34']>0
# p2=df_sql['FUNC1_D@08QU-QU-855I-35']>0
# p3=df_sql['FUNC1_D@08QU-QU-855I-36']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ11'] = df_sql['TEMP1_I@08QU-QU-855I-GQ11'][p][p1][p2][p3]

# # 85
# #Filtro funcionamento do queimador 12
# p=df_sql['FUNC1_D@08QU-QU-855I-37']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-38']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ12'] = df_sql['TEMP1_I@08QU-QU-855I-GQ12'][p][p1]

# # 86
# #Filtro funcionamento do queimador 13
# p=df_sql['FUNC1_D@08QU-QU-855I-39']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-40']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ13'] = df_sql['TEMP1_I@08QU-QU-855I-GQ13'][p][p1]

# # 87
# #Filtro funcionamento do queimador 14
# p=df_sql['FUNC1_D@08QU-QU-855I-41']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-42']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ14'] = df_sql['TEMP1_I@08QU-QU-855I-GQ14'][p][p1]

# # 88
# #Filtro funcionamento do queimador 15
# p=df_sql['FUNC1_D@08QU-QU-855I-43']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-44']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ15'] = df_sql['TEMP1_I@08QU-QU-855I-GQ15'][p][p1]

# # 89
# #Filtro funcionamento do queimador 16
# p=df_sql['FUNC1_D@08QU-QU-855I-45']>0
# p1=df_sql['FUNC1_D@08QU-QU-855I-46']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQ16'] = df_sql['TEMP1_I@08QU-QU-855I-GQ16'][p][p1]

# # 90
# #Filtro funcionamento do queimador A -- solicitar tag func, Aileen não encontrou
# p=df_sql['']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQA'] = df_sql['TEMP1_I@08QU-QU-855I-GQA'][p][p1]

# # 91
# #Filtro funcionamento do queimador B -- solicitar tag func, Aileen não encontrou
# p=df_sql['']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQA'] = df_sql['TEMP1_I@08QU-QU-855I-GQA'][p][p1]

# # 92
# #Filtro funcionamento do queimador C -- solicitar tag func, Aileen não encontrou
# p=df_sql['']>0
# df_sql['TEMP1_I@08QU-QU-855I-GQC'] = df_sql['TEMP1_I@08QU-QU-855I-GQC'][p][p1]

# 93
# cálculo distribuicao_gran - relação granulometrica
df_sql['calc_distribuicao_gran'] = df_sql['GRAN_12,5_PQ_L@08QU'] + df_sql['GRAN_16_PQ_L@08QU'] + df_sql['GRAN_10_PQ_L@08QU'] + df_sql['GRAN_8_PQ_L@08QU']

# 94
#modelo de densidade moinho 
# adicionar, *bomba de retorno do tanque/(bomba retorno do tanque+vazão de saída do tanque) -- verificar correlação com possibilidade 
df_sql['bomba de retorno tanque'] =  df_sql['VAZA1_I@08MO-BP-821I-01']/(df_sql['VAZA1_I@08MO-BP-821I-01']+df_sql['VAZA1_I@08MO-BP-821I-01M1'])


# 95
#modelo de produtividade filtragem, umidade
#média de densidade
df_sql['media de densidade'] =  (((df_sql['DENS1_C@08HO-BP-826I-05'] * df_sql['FUNC1_D@08HO-BP-826I-05M1']) + (df_sql['DENS1_C@08HO-BP-826I-06R'] * df_sql['FUNC1_D@08HO-BP-826I-06RM1']) +(df_sql['DENS1_C@08HO-BP-826I-07'] * df_sql['FUNC1_D@08HO-BP-826I-07M1']) + (df_sql['DENS1_C@08HO-BP-826I-08R'] * df_sql['FUNC1_D@08HO-BP-826I-08RM1']))/ (df_sql['FUNC1_D@08HO-BP-826I-05M1']+ df_sql['FUNC1_D@08HO-BP-826I-06RM1'] +  df_sql['FUNC1_D@08HO-BP-826I-07M1'] + df_sql['FUNC1_D@08HO-BP-826I-08RM1']))
                                                             
# 96
#modelo de produtividade filtragem,umidade
#mediana de rotação
df_sql['mediana de rotacao'] =  np.median([df_sql['ROTA1_I@08FI-FL-827I-01M1'], df_sql['ROTA1_I@08FI-FL-827I-02M1'], df_sql['ROTA1_I@08FI-FL-827I-03M1'], df_sql['ROTA1_I@08FI-FL-827I-04M1'], df_sql['ROTA1_I@08FI-FL-827I-05RM1'], df_sql['ROTA1_I@08FI-FL-827I-06M1'], df_sql['ROTA1_I@08FI-FL-827I-07M1'], df_sql['ROTA1_I@08FI-FL-827I-08M1'], df_sql['ROTA1_I@08FI-FL-827I-09M1'], df_sql['ROTA1_I@08FI-FL-827I-10RM1']],  axis=0)
                                 
# 97
#modelo taxarp
# testar com ou sem 
df_sql['media disco de pelotamento'] =  np.mean([df_sql['ROTA1_I@08PE-BD-840I-01M1'],df_sql['ROTA1_I@08PE-BD-840I-02M1'],df_sql['ROTA1_I@08PE-BD-840I-03M1'],df_sql['ROTA1_I@08PE-BD-840I-04M1'],df_sql['ROTA1_I@08PE-BD-840I-05M1'], df_sql['ROTA1_I@08PE-BD-840I-06M1'],df_sql['ROTA1_I@08PE-BD-840I-07M1'], df_sql['ROTA1_I@08PE-BD-840I-08M1'],df_sql['ROTA1_I@08PE-BD-840I-09M1'],df_sql['ROTA1_I@08PE-BD-840I-10M1'],df_sql['ROTA1_I@08PE-BD-840I-11M1'], df_sql['ROTA1_I@08PE-BD-840I-12M1']],  axis=0)
  
# 98
#modelo taxarp                              
# testar com ou sem 
df_sql['media alimentacao do disco'] = np.mean([df_sql['PESO1_I@08PE-BW-840I-01M1'], df_sql['PESO1_I@08PE-BW-840I-02M1'], df_sql['PESO1_I@08PE-BW-840I-03M1'], df_sql['PESO1_I@08PE-BW-840I-04M1'], df_sql['PESO1_I@08PE-BW-840I-05M1'], df_sql['PESO1_I@08PE-BW-840I-06M1'], df_sql['PESO1_I@08PE-BW-840I-07M1'], df_sql['PESO1_I@08PE-BW-840I-08M1'],df_sql['PESO1_I@08PE-BW-840I-09M1'], df_sql['PESO1_I@08PE-BW-840I-10M1'], df_sql['PESO1_I@08PE-BW-840I-11M1'],df_sql['PESO1_I@08PE-BW-840I-12M1']],  axis=0)                                 
                                 
# 99
#modelo taxarp, abrasao e compressao                              
#trocar o target para os TM
df_sql['media tm'] = np.mean([df_sql['GRAN_OCS_TM@08PE-BD-840I-01'], df_sql['GRAN_OCS_TM@08PE-BD-840I-02'], df_sql['GRAN_OCS_TM@08PE-BD-840I-03'], df_sql['GRAN_OCS_TM@08PE-BD-840I-04'], df_sql['GRAN_OCS_TM@08PE-BD-840I-05'], df_sql['GRAN_OCS_TM@08PE-BD-840I-06'], df_sql['GRAN_OCS_TM@08PE-BD-840I-07'], df_sql['GRAN_OCS_TM@08PE-BD-840I-08'], df_sql['GRAN_OCS_TM@08PE-BD-840I-09'], df_sql['GRAN_OCS_TM@08PE-BD-840I-10'], df_sql['GRAN_OCS_TM@08PE-BD-840I-11'], df_sql['GRAN_OCS_TM@08PE-BD-840I-12']],  axis=0)  
                                                                 
# 100
#modelo SE PR 
df_sql['media_potencia_moinho'] = np.mean([df_sql['POTE1_I@08MO-MO-821I-01M1'],df_sql['POTE1_I@08MO-MO-821I-02M1'],df_sql['POTE1_I@08MO-MO-821I-03M1']],  axis=0)    

# 101
#modelo cfix
#Filtro funcionamento 
p=df_sql['FUNC1_D@08HO-AG-826I-01M1']>0
df_sql['NIVE1_I@08HO-TQ-826I-03'] = df_sql['NIVE1_I@08HO-TQ-826I-03'][p]

# 101
#modelo cfix
#Filtro funcionamento 
p=df_sql['FUNC1_D@08HO-AG-826I-02M1']>0
df_sql['NIVE1_I@08HO-TQ-826I-04'] = df_sql['NIVE1_I@08HO-TQ-826I-04'][p]

# 102
#modelo cfix
#Filtro funcionamento 
p=df_sql['FUNC1_D@08HO-AG-826I-03M1']>0
df_sql['NIVE1_I@08HO-TQ-826I-05'] = df_sql['NIVE1_I@08HO-TQ-826I-05'][p]


# for c_corpo_moedor_especifico, c_corpo_moedor, c_producao in [
#     ('corpo_moedor_especifico_1','PESO2_Q@08MO-TR-821I-02M1-MO01','PESO1_I@08MO-BW-821I-01M1'),
#     ('corpo_moedor_especifico_2','PESO2_Q@08MO-TR-821I-02M1-MO02','PESO1_I@08MO-BW-821I-02M1'),
#     ('corpo_moedor_especifico_3','PESO2_Q@08MO-TR-821I-02M1-MO03','PESO1_I@08MO-BW-821I-03M1'),
# ]:
#     p = df_sql[c_producao]>0
#     df_sql[c_corpo_moedor_especifico] = 0
#     df_sql.loc[p, c_corpo_moedor_especifico] = df_sql[p][c_corpo_moedor] / df_sql[p][c_producao]
df_sql['corpo_moedor_especifico'] = (df_sql['PESO1_Q@08MO-TR-821I-02M1-MO01']+df_sql['PESO1_Q@08MO-TR-821I-02M1-MO02']+df_sql['PESO1_Q@08MO-TR-821I-02M1-MO03']) * df_sql['FUNC1_D@08QU-FR-851I-01M1'] * 1000 / df_sql['PROD_PQ_Y@08QU-ACUM']

# df_sql['corpo_moedor_especifico'] = (
#     ((df_sql['PESO1_Q@08MO-TR-821I-02M1-MO01']*df_sql['FUNC1_D@08QU-FR-851I-01M1'])
#     + (df_sql['PESO1_Q@08MO-TR-821I-02M1-MO02']*df_sql['FUNC1_D@08QU-FR-851I-01M1'])
#     + (df_sql['PESO1_Q@08MO-TR-821I-02M1-MO03']*df_sql['FUNC1_D@08QU-FR-851I-01M1'])) * 1000 
#     / df_sql['PROD_PQ_Y@08QU-ACUM'])


df_sql['taxa_alimentacao_moinhos'] = (
    df_sql['PESO1_I@08MO-BW-821I-01M1']+
    df_sql['PESO1_I@08MO-BW-821I-02M1']+
    df_sql['PESO1_I@08MO-BW-821I-03M1']
)

df_sql['ProducaoPQ_Moagem'] = 0.8762 * df_sql['taxa_alimentacao_moinhos']

# df_sql['antracito'] = df_sql['PESO1_I@08MO-BW-813I-03M1']+df_sql['PESO1_I@08MO-BW-813I-04M1']
df_sql['antracito'] = df_sql['QUIM_CARVAO_PP_L@08PR']*10

df_sql['denominator'] = (0.8762 * (df_sql['PESO1_I@08MO-BW-821I-01M1'] + df_sql['PESO1_I@08MO-BW-821I-02M1'] + df_sql['PESO1_I@08MO-BW-821I-03M1']))
df_sql['calcario'] = df_sql['PESO1_I@08MO-BW-813I-01M1'] / df_sql['denominator']


#df_sql = df_sql.drop(['PESO1_I@08MO-BW-813I-03M1','PESO1_I@08MO-BW-813I-04M1'], axis=1)
# df_sql = df_sql.drop(['PESO1_I@08MO-BW-813I-03M1','PESO1_I@08MO-BW-813I-04M1'], axis=1)

for (var,k,mult) in [
    ('VAZA1_I@08QU-ST-855I-01','11 - TRATAMENTO TERMICO - SPSS - US8', 1),
]:
    prodpq = df_sql['PROD_PQ_Y@08US'].copy()
    p = prodpq > 0
    
    try:
        df_sql = df_sql[p] # removing production equals to 0
    except:
        pass
    
    df_sql[var] = df_sql[var] / df_sql['PROD_PQ_Y@08US'][p] * mult


# for (var,k,mult) in [
# #     ('PESO1_I@08MO-BW-813I-01M1','04 - MOAGEM - SPSS - US8 - PATRICIA', 1),
#     ('antracito','01 a 03 - DADOS SPSS - US8', 1000),
# ]:
#     df_sqlc = df_sql[var].copy()
#     prodpq = df_sql['ProducaoPQ_Moagem'].copy()
#     p = prodpq > 0
#     df_sql[var] = 0
#     df_sql.loc[p, var] = df_sqlc[p] / prodpq[p] * mult


# df_otm_forno['lado'] = -1
df_sql['Media_temp'] = df_sql[['TEMP1_I@08PN-TR-860I-01','TEMP1_I@08PN-TR-860I-02']].mean(1)
# df_otm['Media_posi'] = -1

def df_otm_forno_filtro(x):
    if x['POSI1_I@08QU-GH-851I-41M1'] >= 10 and x['POSI1_I@08QU-GH-851I-42M1'] >= 10:
#         x['lado'] = 2
        
        x['Media_temp'] = (x['TEMP1_I@08PN-TR-860I-01'] + x['TEMP1_I@08PN-TR-860I-02'])/2
#         x['Media_posi'] = (x['POSI1_I@08QU-GH-851I-41M1'] + x['POSI1_I@08QU-GH-851I-42M1'])/2
        
    elif x['POSI1_I@08QU-GH-851I-41M1'] >= 10:
#         x['lado'] = 0
        
        x['Media_temp'] = x['TEMP1_I@08PN-TR-860I-01']
#         x['Media_posi'] = x['POSI1_I@08QU-GH-851I-41M1']
        
    elif x['POSI1_I@08QU-GH-851I-42M1'] >= 10:
#         x['lado'] = 1
        
        x['Media_temp'] = x['TEMP1_I@08PN-TR-860I-02']
#         x['Media_posi'] = x['POSI1_I@08QU-GH-851I-42M1']
    
    return x


## HOT FIX
# df_sql['VAZA1_I@08QU-ST-855I-01'] = df_sql['VAZA1_I@08QU-ST-855I-01'] / df_sql['PROD_PQ_Y@08US'][df_sql['PROD_PQ_Y@08US'] > 0]

# df_sql['bentonita'] = (df_sql['PESO1_I@08MI-LW-832I-01M1'] + df_sql['PESO1_I@08MI-LW-832I-02M1'])/(df_sql['PESO1_I@08MI-BW-832I-01M1'] + df_sql['PESO1_I@08MI-BW-832I-02M1']) * 100 / 0.8693 / df_sql['PROD_PQ_Y@08US']

# df_sql['bentonita'] = (df_sql['PESO1_I@08MI-LW-832I-01M1'] + df_sql['PESO1_I@08MI-LW-832I-02M1'])/ df_sql['PROD_PQ_Y@08US'] * 1000

df_sql['bentonita'] = (df_sql['PESO1_I@08MI-LW-832I-01M1']+df_sql['PESO1_I@08MI-LW-832I-02M1'])/(df_sql['PESO1_I@08MI-BW-832I-01M1']+df_sql['PESO1_I@08MI-BW-832I-02M1'])*df_sql['FUNC1_D@08QU-FR-851I-01M1']*1000

## Calcario dividido duas vezes
# for (var,k,mult) in [
#     ('PESO1_I@08MO-BW-813I-01M1','04 - MOAGEM - SPSS - US8 - PATRICIA', 1),
# #     ('antracito','01 a 03 - DADOS SPSS - US8', 1000),
# ]:
#     dfc = df_sql[var].copy()
#     prodpq = df_sql['ProducaoPQ_Moagem'].copy()
#     p = prodpq > 0
#     df_sql[var] = 0
#     df_sql.loc[p, var] = dfc[p] / prodpq[p] * mult


# calculo da potencia de filtragem - pedido pelo rubens

# df_sql['FUNC1_Q@08FI-BV-827I-01M1','FUNC1_Q@08FI-BV-827I-02M1','FUNC1_Q@08FI-BV-827I-03M1','FUNC1_Q@08FI-BV-827I-04M1',       'FUNC1_Q@08FI-BV-827I-05RM1','FUNC1_Q@08FI-BV-827I-06M1','FUNC1_Q@08FI-BV-827I-07M1','FUNC1_Q@08FI-BV-827I-08M1',       'FUNC1_Q@08FI-BV-827I-09M1','FUNC1_Q@08FI-BV-827I-10RM1']

# aguardando inclusao no datalake


# potencia de filtragem

temp = pd.DataFrame()
func_pot = ['POTE1_Q@08FI-BV-827I-01M1', 
            'POTE1_Q@08FI-BV-827I-02M1',
            'POTE1_Q@08FI-BV-827I-03M1',
            'POTE1_Q@08FI-BV-827I-04M1',
            'POTE1_Q@08FI-BV-827I-05RM1',
            'POTE1_Q@08FI-BV-827I-06M1',
            'POTE1_Q@08FI-BV-827I-07M1',
            'POTE1_Q@08FI-BV-827I-08M1',
            'POTE1_Q@08FI-BV-827I-09M1',
            'POTE1_Q@08FI-BV-827I-10RM1']


for tag in func_pot:
    temp[tag] = df_sql[tag].diff()
    
temp.fillna(0, inplace = True)

temp['potencia_filtragem'] = temp[func_pot].sum(axis=1)

# produção seca

temp['prod_seca'] = (df_sql['PESO1_I@08FI-TR-827I-01M1'] + df_sql['PESO1_I@08FI-TR-827I-02M1'])*(1 - df_sql['UMID_H2O_PR_L@08FI']/100)

df_sql['Consumo_energia_especifica'] = temp['potencia_filtragem']/temp['prod_seca']

del temp


df_sql.loc[df_sql['Consumo_energia_especifica'] > 3.12, 'Consumo_energia_especifica'] = np.nan


dfs = {}
for k, v in process_tag_map.items():
  columns_to_add = set(v).intersection(df_sql.columns)
  dfs[k] = df_sql.loc[:, columns_to_add].copy()


# datasets_limits_path = 'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/Usina08/Otimizacao/files/'
# limits = load_adls_json(datasets_limits_path, 'datasets_limits.json')

limits_temp = limits.copy()

for key in limits_temp.keys():
    limits[key.replace('.csv', '')] = limits.pop(key)


limits['08 - PRENSA - SPSS - US8']['PESO1_I@08PR-BW-822I-01M1'] = (0, None)
limits['04 - MOAGEM - SPSS - US8 - PATRICIA']['PESO1_I@08MO-BW-821I-02M1_DP'] = (0, None)
limits['04 - MOAGEM - SPSS - US8 - PATRICIA']['PESO1_I@08MO-BW-821I-03M1_DP'] = (0, None)
limits['07 - FILTRAGEM - SPSS - US8']['PESO1_I@08FI-TR-827I-01M1'] = (0, None)
limits['08 - PRENSA - SPSS - US8']['TORQ1_I@08PR-RP-822I-01M1'] = (0, None)
limits['08 - PRENSA - SPSS - US8']['TORQ1_I@08PR-RP-822I-01M2'] = (0, None)
limits['11 - TRATAMENTO TERMICO - SPSS - US8']['PROD_PC_I@08US'][0] = 0
limits['11 - TRATAMENTO TERMICO - SPSS - US8']['TORQ1_I@08QU-PF-852I-02M1'] = (0, None)
limits['11 - TRATAMENTO TERMICO - SPSS - US8']['TORQ1_I@08QU-PF-852I-04M1'] = (0, None)
limits['11 - TRATAMENTO TERMICO - SPSS - US8']['TORQ1_I@08QU-PF-852I-05M1'] = (0, None)
limits['11 - TRATAMENTO TERMICO - SPSS - US8']['TORQ1_I@08QU-PF-852I-08M1'] = (0, None)
limits['11 - TRATAMENTO TERMICO - SPSS - US8']['PESO1_I@08PN-TR-860I-09M1'] = (0, None)
limits['11 - TRATAMENTO TERMICO - SPSS - US8']['VELO2_C@08QU-FR-851I-01M1'] = (0, None)

limits['10 - AGLOMERACAO - SPSS - US8']['PROD_PQ_Y@08US'] = (0, None)
limits['11 - TRATAMENTO TERMICO - SPSS - US8']['PROD_PQ_Y@08US'] = (0, None)
limits['12 - M.AMBIENTE - SPSS - US8']['VOLT1_I@08QU-PP-871I-02-PAT02']=None

# for t in dfs['10 - AGLOMERACAO - SPSS - US8'][[x for x in datasets['distribuicao gran'].columns if 'GRAN_OCS_TM' in x]]:
#   if 'Y' not in t:
#     limits['10 - AGLOMERACAO - SPSS - US8'][t] = (dfs['10 - AGLOMERACAO - SPSS - US8'][t].quantile(0.15), None)


dfs_tmp = {}
for k, df in dfs.items():
    dfs_tmp[k] = df[~df.index.duplicated()]
dfs = dfs_tmp
print('isso nao faz sentido!!!')


volt_exclude = ['VOLT1_I@08QU-PP-871I-01-PAT0' + str(i) for i in range(1,7)]
volt_exclude.extend(['VOLT2_I@08QU-PP-871I-01-PAT0' + str(i) for i in range(1,7)]) 
# se os limites forem aplicados estas tags volt ficam impossibilitadas de serem desligadas na otimizacao (min > 0)

# limites de pressao ajustados manualmente no json e em mocedels_v2
limits_exclude = ['PRES1_I@08QU-WB-851I-01',
                'PRES1_I@08QU-WB-851I-02',
                'PRES1_I@08QU-WB-851I-04',
                'PRES1_I@08QU-WB-851I-05',
                'PRES1_I@08QU-WB-851I-06',
                'PRES1_I@08QU-WB-851I-07',
                'PRES1_I@08QU-WB-851I-08',
                'PRES1_I@08QU-WB-851I-09',
                'PRES1_I@08QU-WB-851I-10',
                'PRES1_I@08QU-WB-851I-11',
                'PRES1_I@08QU-WB-851I-12',
                'PRES1_I@08QU-WB-851I-13',
                'PRES1_I@08QU-WB-851I-21',
                'PRES1_I@08QU-WB-851I-27',
                'PRES1_I@08QU-WB-851I-31']
limits_exclude.extend(volt_exclude)


for k, df in dfs.items():
    for t in df.columns:
        if (t in limits[k]) and (limits[k][t] is not None) and (t not in limits_exclude):
            
            lmin, lmax = limits[k][t]
            
            if lmin is not None:
                df[t] = df[t][df[t]>=lmin]

            if lmax is not None:
                df[t] = df[t][df[t]<=lmax]


# MAGIC %md
# MAGIC # gera docionario datasets


def find_col(dict_object, col_name):
    """busca coluna em dicionario, no caso o datasets:
        chave : string
            nome do modelo
        valor : pandas.DataFrame
            variáveis do modelo onde cada linha equivale a 1 hora de captura dos dados
    """
    for model, df in dict_object.items():
        for c in df:
            if isinstance(c, int): continue
            if c.startswith(col_name):
                print(model, c)


datasets = {}

for base in datasets_confs:    
    dfs_variaveis = []
    for k, tags in base['variaveis']:
        for t in tags:
            if t in dfs[k]:
                dfs_variaveis.append(dfs[k][[t]])
            else:
                log.info(f'{t} ,\t, NAO ENCONTRADO')

        if len(tags) == 0:
            if k == '10 - AGLOMERACAO - SPSS - US8':
                dfs_variaveis.append(dfs[k].drop(['PROD_PQ_Y@08US', 'PROD_PC_I@08US'], axis=1))
            else:
                dfs_variaveis.append(dfs[k])
    
    for qual, conf in base['targets'].items():
        if qual in ['distribuicao gran', 'custo_distribuicao gran']:
            target = dfs[conf[0]][conf[1]].dropna().sum(axis=1)
        elif qual in ['torque']:
            target = dfs[conf[0]][conf[1]].dropna().mean(axis=1).rename('TORQ1')
        elif qual == 'taxarp':
            target = dfs[conf[0]][conf[1]].dropna()
        else:
            target = dfs[conf[0]][[conf[1]]]
        
        df_target = target
        datasets[qual] = pd.concat(dfs_variaveis, axis=1)

        base_status = [x for x in base['status'] if x in datasets[qual].columns]
        if len(set(base['status'])-set(base_status)) > 0:
            log.info(f'{set(base["status"])-set(base_status)}, \t, NAO ENCONTRADOS')
        datasets[qual]['status'] = (datasets[qual][base_status].max(axis=1) >= 1).astype(int)
        
        if qual in ['GRAN PR', 'SE PR', 'SE PP']+['abrasao','compressao','basicidade','finos','distribuicao gran']:
            for suf in ['CA', 'CN', 'CE', 'BR']:
                cs = [c for c in datasets[qual].columns if c.endswith(suf) and c != 'SFBR']
                if len(cs) > 0:
                    datasets[qual]['minerio_'+suf] = datasets[qual][cs].sum(axis=1)
                    datasets[qual] = datasets[qual].drop(cs, axis=1)

        if qual in ['SE PP']:
            #aguardando tags serem integradas, pegar maior valor 
            #ts_torque = ['TORQ2_I@08PR-RP-822I-01M1','TORQ2_I@08PR-RP-822I-01M2']
            ts_torque = ['TORQ1_I@08PR-RP-822I-01M2','TORQ1_I@08PR-RP-822I-01M1']
            datasets[qual]['TORQ1'] = datasets[qual][ts_torque].mean(axis=1)
#             datasets[qual] = datasets[qual].drop(ts_torque, axis=1)
        
        if qual == 'basicidade':       
            tmp = ['QUIM_CAO_PP_L@08PR','QUIM_SIO2_MA_L@08PA','SFBR','P DP','PPC Pilha','GRAN MA','MGO MED','QUIM_CARVAO_PP_L@08PR','QUIM_CFIX_PP_L@08PR','SIO2_OCS_I@08MO-CALCARIO','minerio_CA','minerio_CN','AL2O3 MED','MN MED','SIO2 DP','QUIM_CAO_MA_L@08PA DP','minerio_CE','TIO2 DP','TIO2 MED','AL2O3 DP','minerio_BR','MGO DP','MN DP']            
            tmp += ['status']
            tmp = [x for x in datasets[qual].columns if x in tmp]
            datasets[qual] = datasets[qual][tmp]
            removerTAGs = [x for x in datasets[qual].columns if x in ['QUIM_CAO_PP_L@08PR','P DP','SIO2 DP','QUIM_CAO_MA_L@08PA DP','TIO2 DP','AL2O3 DP','MGO DP','MN DP']]
            datasets[qual] = datasets[qual].drop(removerTAGs, axis=1)
        
        if qual != 'basicidade':
            if 'qtde_equipamentos' in base:
                datasets[qual][base['qtde_equipamentos'][0]] = (datasets[qual][base['qtde_equipamentos'][1]] >= 0.5).sum(axis=1)
                
            if 'para_remover' in base:
#                 for t_para_remover in base['para_remover']: assert t_para_remover in datasets[qual].columns, (qual, t_para_remover)
                removerTAGs = [x for x in datasets[qual].columns if x in base['para_remover']]
                datasets[qual] = datasets[qual].drop(removerTAGs, axis=1)
        log.info(f'{qual}, {datasets[qual].shape}, {df_target.shape}')
        datasets[qual] = pd.concat([datasets[qual], df_target], axis=1, sort=True)
        datasets[qual].index.name = 'Processo' # tava dando erro na 'distribuicao gran' apos o dropna()


# solucao temporaria, nao entendi de onde vem essa coluna. Ela surge em distribuicao gran como 0, depois renomeado para vazio
for model, df in datasets.items():
    if 0 in df.columns:
        df.drop(0, axis=1, inplace=True)
        datasets[model] = df.copy()


# cria modelos de temp_forno e temp_precipitador

datasets['temp_forno'] = datasets['abrasao'][['TEMP1_I@08PN-TR-860I-01', 'TEMP1_I@08PN-TR-860I-02']].copy()
datasets['temp_forno']['Media_temp'] = df_sql['Media_temp']
datasets['temp_precipitador'] = datasets['abrasao'][['TEMP1_I@08QU-PP-871I-01', 'TEMP1_I@08QU-PP-871I-02', 'TEMP1_I@08QU-PP-871I-03']]


# cria modelo de relacao gran

datasets['relacao gran'] = datasets['distribuicao gran'].copy()

df_sql['rel_gran'] = ((df_sql['GRAN_12,5_PQ_L@08QU']+df_sql['GRAN_16_PQ_L@08QU']) / (df_sql['GRAN_10_PQ_L@08QU']+df_sql['GRAN_8_PQ_L@08QU'])).fillna(method='bfill').fillna(method='ffill').fillna(0)

datasets['relacao gran'] = datasets['relacao gran'].join(df_sql['rel_gran']).copy()
datasets['relacao gran'] = datasets['relacao gran'][(df_sql['rel_gran'] >= 0.8) & (df_sql['rel_gran'] <= 1.2)]


for k, v in datasets.items():
  if ('status' in datasets[k].columns) and (((datasets[k]['status'] == 0).sum() == datasets[k].shape[0])):
    log.info(k)
    datasets[k]['status'] = 1


datasets_originais = datasets.copy()


for k, df in datasets_originais.items():
    n_coluns = datasets_originais[k].shape[1]
    datasets_originais[k] = df.loc[:, ~df.columns.duplicated()]
    if n_coluns != datasets_originais[k].shape[1]: log.info(k)


# MAGIC %md
# MAGIC # DEBUG: celula muito estranha


# datasets = {}

# isso é definido em modelos-preditivo!!!
# test_size = { q: 0.2 for q in datasets_originais.keys() }
# cv_size = 0.3 # em relacao ao tamanho do treino, sem considerar o test
# cv_n = 3

# for q in datasets_originais.keys():
#     datasets[q] = datasets_originais[q].copy()
    
#     ix = datasets[q].index.sort_values()
#     train_threshold = ix[int(len(ix) * (1-test_size[q]))]
#     datasets[q] = datasets[q][datasets[q].index <= train_threshold]

datasets = datasets_originais.copy() 
log.debug('recupera datasets, mas datasets_originais ja era cópia de datasets!!!!')


# MAGIC %md
# MAGIC # Gerando Modelos


# MAGIC %md
# MAGIC #### Gerando os modelos de rota_disco


for i in range(1,13):
    v_rota = 'ROTA1_I@08PE-BD-840I-{:02}M1'.format(i)
    v_peso = 'PESO1_I@08PE-BW-840I-{:02}M1'.format(i)
    v_tm = 'GRAN_OCS_TM@08PE-BD-840I-{:02}'.format(i)
    v_func = 'FUNC1_D@08PE-BD-840I-{:02}M1'.format(i)
    
    datasets['rota_disco_'+str(i)] = datasets['finos'][[v_peso, v_tm, v_func, v_rota]].rename(columns={v_func:'status'})


# MAGIC %md
# MAGIC #### Gerando o modelo de temperatura de recirculação


datasets['temp_recirc'] = datasets['finos'][['PROD_PQ_Y@08US', 'ROTA1_I@08QU-PF-852I-01M1', 'ROTA1_I@08QU-PF-852I-07M1', 'ROTA1_I@08QU-PF-852I-08M1', 'TEMP1_I@08QU-QU-855I-GQ01', 'TEMP1_I@08QU-QU-855I-GQ03', 'TEMP1_I@08QU-QU-855I-GQ04', 'TEMP1_I@08QU-QU-855I-GQ05', 'TEMP1_I@08QU-QU-855I-GQ06', 'TEMP1_I@08QU-QU-855I-GQ07', 'TEMP1_I@08QU-QU-855I-GQ08', 'TEMP1_I@08QU-QU-855I-GQ09', 'TEMP1_I@08QU-QU-855I-GQ10', 'TEMP1_I@08QU-QU-855I-GQ11', 'TEMP1_I@08QU-QU-855I-GQ12', 'TEMP1_I@08QU-QU-855I-GQ13', 'TEMP1_I@08QU-QU-855I-GQ14', 'TEMP1_I@08QU-QU-855I-GQ15', 'TEMP1_I@08QU-QU-855I-GQ16','status','TEMP1_I@08QU-HO-851I-01']]


# MAGIC %md
# MAGIC #### Gerando os modelos de Densidade do Moinho


# MAGIC %md
# MAGIC criado em 20220705 a pedido do Rodrigo - 
# MAGIC cópia do modelo de SE PR para cada moinho específico


# criando dataset
datasets['dens_moinho_1'] = datasets['SE PR'].drop(columns=['DENS1_C@08MO-MO-821I-01', 'DENS1_C@08MO-MO-821I-02', 'DENS1_C@08MO-MO-821I-03', 'FUNC1_D@08MO-MO-821I-02M1',
                                                            'FUNC1_D@08MO-MO-821I-03M1', 'NIVE1_I@08MO-TQ-821I-02', 'NIVE1_I@08MO-TQ-821I-03', 'PESO1_I@08MO-BW-821I-02M1',
                                                            'PESO1_I@08MO-BW-821I-03M1', 'PESO2_Q@08MO-TR-821I-02M1-MO02', 'PESO2_Q@08MO-TR-821I-02M1-MO03',
                                                            'POTE1_I@08MO-MO-821I-02M1', 'POTE1_I@08MO-MO-821I-03M1', 'qtde_moinhos', 'ProducaoPQ_Moagem',
                                                            'NIVE1_I@08MO-SI-813I-06']).copy()
#target
datasets['dens_moinho_1']['DENS1_C@08MO-MO-821I-01'] = datasets['SE PR']['DENS1_C@08MO-MO-821I-01'].copy()

# retirando func -> transformando ele em status para ser dropado no treino (teoricamente, pelo datasets_confs, ele já é o status, mas, quando fui olhar o describe, estava um pouco diferente. por isso to fazendo essa substituição)
datasets['dens_moinho_1']['status'] = datasets['dens_moinho_1']['FUNC1_D@08MO-MO-821I-01M1'].copy()
# datasets['dens_moinho_1'] = datasets['dens_moinho_1'][datasets['dens_moinho_1']['FUNC1_D@08MO-MO-821I-01M1'] == 1].copy()
datasets['dens_moinho_1'].drop(columns = ['FUNC1_D@08MO-MO-821I-01M1'], inplace = True)


# criando dataset
datasets['dens_moinho_2'] = datasets['SE PR'].drop(columns=['DENS1_C@08MO-MO-821I-01', 'DENS1_C@08MO-MO-821I-02', 'DENS1_C@08MO-MO-821I-03', 'FUNC1_D@08MO-MO-821I-01M1',
                                                            'FUNC1_D@08MO-MO-821I-03M1', 'NIVE1_I@08MO-TQ-821I-01', 'NIVE1_I@08MO-TQ-821I-03',
                                                            'PESO1_I@08MO-BW-821I-01M1', 'PESO1_I@08MO-BW-821I-03M1', 'PESO2_Q@08MO-TR-821I-02M1-MO01',
                                                            'PESO2_Q@08MO-TR-821I-02M1-MO03', 'POTE1_I@08MO-MO-821I-01M1', 'POTE1_I@08MO-MO-821I-03M1', 
                                                            'qtde_moinhos', 'ProducaoPQ_Moagem', 'NIVE1_I@08MO-SI-813I-06']).copy()
#target
datasets['dens_moinho_2']['DENS1_C@08MO-MO-821I-02'] = datasets['SE PR']['DENS1_C@08MO-MO-821I-02'].copy()

# retirando func -> transformando ele em status para ser dropado no treino (teoricamente, pelo datasets_confs, ele já é o status, mas, quando fui olhar o describe, estava um pouco diferente. por isso to fazendo essa substituição)
datasets['dens_moinho_2']['status'] = datasets['dens_moinho_2']['FUNC1_D@08MO-MO-821I-02M1'].copy()
# datasets['dens_moinho_2'] = datasets['dens_moinho_2'][datasets['dens_moinho_2']['FUNC1_D@08MO-MO-821I-02M1'] == 1].copy()
datasets['dens_moinho_2'].drop(columns = ['FUNC1_D@08MO-MO-821I-02M1'], inplace = True)


# criando dataset
datasets['dens_moinho_3'] = datasets['SE PR'].drop(columns=['DENS1_C@08MO-MO-821I-01', 'DENS1_C@08MO-MO-821I-02', 'DENS1_C@08MO-MO-821I-03', 'FUNC1_D@08MO-MO-821I-01M1',
                                                            'FUNC1_D@08MO-MO-821I-02M1', 'NIVE1_I@08MO-TQ-821I-01', 'NIVE1_I@08MO-TQ-821I-02',
                                                            'PESO1_I@08MO-BW-821I-01M1', 'PESO1_I@08MO-BW-821I-02M1', 'PESO2_Q@08MO-TR-821I-02M1-MO01',
                                                            'PESO2_Q@08MO-TR-821I-02M1-MO02', 'POTE1_I@08MO-MO-821I-01M1', 'POTE1_I@08MO-MO-821I-02M1', 
                                                            'qtde_moinhos', 'ProducaoPQ_Moagem', 'NIVE1_I@08MO-SI-813I-06']).copy()
#target
datasets['dens_moinho_3']['DENS1_C@08MO-MO-821I-03'] = datasets['SE PR']['DENS1_C@08MO-MO-821I-03'].copy()

# retirando func -> transformando ele em status para ser dropado no treino (teoricamente, pelo datasets_confs, ele já é o status, mas, quando fui olhar o describe, estava um pouco diferente. por isso to fazendo essa substituição)
datasets['dens_moinho_3']['status'] = datasets['dens_moinho_3']['FUNC1_D@08MO-MO-821I-03M1'].copy()
# datasets['dens_moinho_3'] = datasets['dens_moinho_3'][datasets['dens_moinho_3']['FUNC1_D@08MO-MO-821I-03M1'] == 1].copy()
datasets['dens_moinho_3'].drop(columns = ['FUNC1_D@08MO-MO-821I-03M1'], inplace = True)


#tags ainda não estão no datalake - pedido da aileen
# add_model_tags(datasets, df_sql, 'dens_moinho_1', ['VAZA1_I@08MO-BP-821I-01M1', 'VAZA1_I@08AP-BP-875I-01', 'DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-01', 'VAZA1_I@08MO-MO-821I-01', 'VAZA1_I@08MO-TQ-821I-01', 'UMID_H2O_AM_L@08MI'])
# add_model_tags(datasets, df_sql, 'dens_moinho_2', ['VAZA1_I@08MO-BP-821I-02M1', 'VAZA1_I@08AP-BP-875I-01', 'DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-02', 'VAZA1_I@08MO-MO-821I-02', 'VAZA1_I@08MO-TQ-821I-02'])
# add_model_tags(datasets, df_sql, 'dens_moinho_3', ['VAZA1_I@08MO-BP-821I-03M1', 'VAZA1_I@08AP-BP-875I-01', 'DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-03', 'VAZA1_I@08MO-MO-821I-03', 'VAZA1_I@08MO-TQ-821I-03'])


add_model_tags(datasets, df_sql, 'dens_moinho_1', ['VAZA1_I@08MO-BP-821I-01M1', 'VAZA1_I@08AP-BP-875I-01', 'DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-01', 'VAZA1_I@08MO-MO-821I-01', 'VAZA1_I@08MO-TQ-821I-01'])
add_model_tags(datasets, df_sql, 'dens_moinho_2', ['VAZA1_I@08MO-BP-821I-02M1', 'VAZA1_I@08AP-BP-875I-01', 'DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-02', 'VAZA1_I@08MO-MO-821I-02', 'VAZA1_I@08MO-TQ-821I-02'])
add_model_tags(datasets, df_sql, 'dens_moinho_3', ['VAZA1_I@08MO-BP-821I-03M1', 'VAZA1_I@08AP-BP-875I-01', 'DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-03', 'VAZA1_I@08MO-MO-821I-03', 'VAZA1_I@08MO-TQ-821I-03'])


# MAGIC %md
# MAGIC #### continua


base = datasets['gas'][datasets['gas'].columns[:-1]]
for indx, tgt in enumerate(datasets['temp_precipitador'].columns):
    modelo = "temp_precipitador_{}".format(indx+1)
    
    log.info(f'Modelo, {modelo}, |, {tgt}')
    
    datasets[modelo] = base.copy()
    datasets[modelo][tgt] = datasets['temp_precipitador'][tgt].copy()
    
if 'temp_precipitador' in datasets.keys():
    del datasets['temp_precipitador']


base = datasets['gas'][datasets['gas'].columns[:-1]]
df_temp_forno = datasets['temp_forno']

target = 'Media_temp'
df_temp_forno = df_temp_forno[df_temp_forno.columns.difference([
    'TEMP1_I@08PN-TR-860I-01',
    'TEMP1_I@08PN-TR-860I-02',
    'Media_posi',
#     'Media_temp'
])]

df_forno = pd.concat([base, df_temp_forno], axis=1)

df_forno = df_forno[[*df_forno.columns.difference([target]), target]]
df_forno['status'] = 1

datasets['temp_forno'] = df_forno


datasets['particulados1'] = datasets['particulados1'].drop(['VOLT2_I@08QU-PP-871I-02-PAT04', 'VOLT2_I@08QU-PP-871I-02-PAT03', 'VOLT1_I@08QU-PP-871I-02-PAT06', 'VOLT1_I@08QU-PP-871I-02-PAT05', 'VOLT1_I@08QU-PP-871I-02-PAT03', 'VOLT1_I@08QU-PP-871I-02-PAT04'], axis=1)


add_model_tags(datasets, df_sql, 'produtividade filtragem', ['UMID_H2O_PR_L@08FI'])


datasets_teste = {}
for k, v in datasets.items():
  log.info(k)
  datasets_teste[k] = v.rename(columns=lambda x: re.sub(r'^[\d.-]+\s*', '', str(x)))
  
datasets = datasets_teste.copy()


exclude_limit_tags = ['VAZA1_I@08QU-ST-855I-01']
for k, df in datasets.items():
  for tag in df.columns:
    if ('FUNC1' not in tag) and ('status' not in tag) and (tag not in exclude_limit_tags):
      datasets[k][tag] = df[tag][(df[tag] >= df[tag].quantile(0.05)) & (df[tag] <= df[tag].quantile(0.95))]
  
  datasets[k] = datasets[k].replace(9999, np.nan)


busca = ['ABRA_-0,5_PQ_L@08QU',
 'GRAN_-0,045_PR_L@08FI',
 'GRAN_10_PQ_L@08QU',
 'GRAN_12,5_PQ_L@08QU',
 'GRAN_16_PQ_L@08QU',
 'GRAN_8_PQ_L@08QU',
 'PARTIC_I@08QU-CH-854I-01',
 'PARTIC_I@08QU-CH-854I-02',
 'PARTIC_I@08QU-CH-854I-03',
 'QUIM_BAS2_PQ_L@08QU',
 'QUIM_CFIX_PP_L@08PR',
 'UMID_H2O_PR_L@08FI']

for tag in busca:
    for model in datasets.keys():

        if tag in datasets[model].columns:
            if tag == datasets[model].columns[-1]:
                print(tag, '->', model, 'target')
            else:
                print(tag, '->', model)


# MAGIC %md
# MAGIC # adicoes e remocoes de tags sob demanda
# MAGIC * Adições e remoções, além de realizar cópia de modelo


gran_ocs_remover = ['GRAN_OCS_+16@08PE-BD-840I-01',	'GRAN_OCS_+16@08PE-BD-840I-02',	'GRAN_OCS_+16@08PE-BD-840I-03',	'GRAN_OCS_+16@08PE-BD-840I-04',	'GRAN_OCS_+16@08PE-BD-840I-05',	'GRAN_OCS_+16@08PE-BD-840I-06',	'GRAN_OCS_+16@08PE-BD-840I-07',	'GRAN_OCS_+16@08PE-BD-840I-08',	'GRAN_OCS_+16@08PE-BD-840I-09',	'GRAN_OCS_+16@08PE-BD-840I-10',	'GRAN_OCS_+16@08PE-BD-840I-11',	'GRAN_OCS_+16@08PE-BD-840I-12',	'GRAN_OCS_10-12@08PE-BD-840I-01',	'GRAN_OCS_10-12@08PE-BD-840I-02',	'GRAN_OCS_10-12@08PE-BD-840I-03',	'GRAN_OCS_10-12@08PE-BD-840I-04',	'GRAN_OCS_10-12@08PE-BD-840I-05',	'GRAN_OCS_10-12@08PE-BD-840I-06',	'GRAN_OCS_10-12@08PE-BD-840I-07',	'GRAN_OCS_10-12@08PE-BD-840I-08',	'GRAN_OCS_10-12@08PE-BD-840I-09',	'GRAN_OCS_10-12@08PE-BD-840I-10',	'GRAN_OCS_10-12@08PE-BD-840I-11',	'GRAN_OCS_10-12@08PE-BD-840I-12',	'GRAN_OCS_10-16@08PE-BD-840I-01',	'GRAN_OCS_10-16@08PE-BD-840I-02',	'GRAN_OCS_10-16@08PE-BD-840I-03',	'GRAN_OCS_10-16@08PE-BD-840I-04',	'GRAN_OCS_10-16@08PE-BD-840I-05',	'GRAN_OCS_10-16@08PE-BD-840I-06',	'GRAN_OCS_10-16@08PE-BD-840I-07',	'GRAN_OCS_10-16@08PE-BD-840I-08',	'GRAN_OCS_10-16@08PE-BD-840I-09',	'GRAN_OCS_10-16@08PE-BD-840I-10',	'GRAN_OCS_10-16@08PE-BD-840I-11',	'GRAN_OCS_10-16@08PE-BD-840I-12',	'GRAN_OCS_12-16@08PE-BD-840I-01',	'GRAN_OCS_12-16@08PE-BD-840I-02',	'GRAN_OCS_12-16@08PE-BD-840I-03',	'GRAN_OCS_12-16@08PE-BD-840I-04',	'GRAN_OCS_12-16@08PE-BD-840I-05',	'GRAN_OCS_12-16@08PE-BD-840I-06',	'GRAN_OCS_12-16@08PE-BD-840I-07',	'GRAN_OCS_12-16@08PE-BD-840I-08',	'GRAN_OCS_12-16@08PE-BD-840I-09',	'GRAN_OCS_12-16@08PE-BD-840I-10',	'GRAN_OCS_12-16@08PE-BD-840I-11',	'GRAN_OCS_12-16@08PE-BD-840I-12',	'GRAN_OCS_16-18@08PE-BD-840I-01',	'GRAN_OCS_16-18@08PE-BD-840I-02',	'GRAN_OCS_16-18@08PE-BD-840I-03',	'GRAN_OCS_16-18@08PE-BD-840I-04',	'GRAN_OCS_16-18@08PE-BD-840I-05',	'GRAN_OCS_16-18@08PE-BD-840I-06',	'GRAN_OCS_16-18@08PE-BD-840I-07',	'GRAN_OCS_16-18@08PE-BD-840I-08',	'GRAN_OCS_16-18@08PE-BD-840I-09',	'GRAN_OCS_16-18@08PE-BD-840I-10',	'GRAN_OCS_16-18@08PE-BD-840I-11',	'GRAN_OCS_16-18@08PE-BD-840I-12',	'GRAN_OCS_5-8@08PE-BD-840I-01',	'GRAN_OCS_5-8@08PE-BD-840I-02',	'GRAN_OCS_5-8@08PE-BD-840I-03',	'GRAN_OCS_5-8@08PE-BD-840I-04',	'GRAN_OCS_5-8@08PE-BD-840I-05',	'GRAN_OCS_5-8@08PE-BD-840I-06',	'GRAN_OCS_5-8@08PE-BD-840I-07',	'GRAN_OCS_5-8@08PE-BD-840I-08',	'GRAN_OCS_5-8@08PE-BD-840I-09',	'GRAN_OCS_5-8@08PE-BD-840I-10',	'GRAN_OCS_5-8@08PE-BD-840I-11',	'GRAN_OCS_5-8@08PE-BD-840I-12',	'GRAN_OCS_8-10@08PE-BD-840I-01',	'GRAN_OCS_8-10@08PE-BD-840I-02',	'GRAN_OCS_8-10@08PE-BD-840I-03',	'GRAN_OCS_8-10@08PE-BD-840I-04',	'GRAN_OCS_8-10@08PE-BD-840I-05',	'GRAN_OCS_8-10@08PE-BD-840I-06',	'GRAN_OCS_8-10@08PE-BD-840I-07',	'GRAN_OCS_8-10@08PE-BD-840I-08',	'GRAN_OCS_8-10@08PE-BD-840I-09',	'GRAN_OCS_8-10@08PE-BD-840I-10',	'GRAN_OCS_8-10@08PE-BD-840I-11',	'GRAN_OCS_8-10@08PE-BD-840I-12',	'GRAN_OCS_TM_Y@08PE-BD-840I']


# MAGIC %md
# MAGIC ## Abrasão, compressão


#abrasao=compreensão

tags_compressao_abrasao = ['- DIF PRODUTIVI EFETIVA - VIRTUAL - CALC - US8',	'bentonita',	'CONS ESP VENT TOTAL - US8',	'CONS ESPEC EE VENT - US8',	'CONS1_Y@08QU-PF-852I-01M1',	'CONS1_Y@08QU-PF-852I-02M1',	'CONS1_Y@08QU-PF-852I-03M1',	'CONS1_Y@08QU-PF-852I-04M1',	'CONS1_Y@08QU-PF-852I-05M1',	'CONS1_Y@08QU-PF-852I-06M1',	'CONS1_Y@08QU-PF-852I-07M1',	'CONS1_Y@08QU-PF-852I-08M1',	'CONS1_Y@08QU-VENT',	'FUNC1_D@08PE-BD-840I-01M1',	'FUNC1_D@08PE-BD-840I-02M1',	'FUNC1_D@08PE-BD-840I-03M1',	'FUNC1_D@08PE-BD-840I-04M1',	'FUNC1_D@08PE-BD-840I-05M1',	'FUNC1_D@08PE-BD-840I-06M1',	'FUNC1_D@08PE-BD-840I-07M1',	'FUNC1_D@08PE-BD-840I-08M1',	'FUNC1_D@08PE-BD-840I-09M1',	'FUNC1_D@08PE-BD-840I-10M1',	'FUNC1_D@08PE-BD-840I-11M1',	'FUNC1_D@08PE-BD-840I-12M1',	'GANHO PRENSA - US8',	'MAIOR - MENOR ALT CAMADA',	'NIVE1_I@08MO-SI-813I-06',	'PERMEABILIDADE CV1 - CALC - US8',	'PERMEABILIDADE CV10 - CALC - US8',	'PERMEABILIDADE CV11 - CALC - US8',	'PERMEABILIDADE CV12 - CALC - US8',	'PERMEABILIDADE CV13 - CALC - US8',	'PERMEABILIDADE CV14 - CALC - US8',	'PERMEABILIDADE CV15 - CALC - US8',	'PERMEABILIDADE CV16 - CALC - US8',	'PERMEABILIDADE CV17 - CALC - US8',	'PERMEABILIDADE CV18 - CALC - US8',	'PERMEABILIDADE CV19 - CALC - US8',	'PERMEABILIDADE CV2 - CALC - US8',	'PERMEABILIDADE CV20 - CALC - US8',	'PERMEABILIDADE CV21 - CALC - US8',	'PERMEABILIDADE CV27 - CALC - US8',	'PERMEABILIDADE CV31 - CALC - US8',	'PERMEABILIDADE CV3A - CALC - US8',	'PERMEABILIDADE CV3B - CALC - US8',	'PERMEABILIDADE CV4 - CALC - US8',	'PERMEABILIDADE CV5 - CALC - US8',	'PERMEABILIDADE CV6 - CALC - US8',	'PERMEABILIDADE CV7 - CALC - US8',	'PERMEABILIDADE CV8 - CALC - US8',	'PERMEABILIDADE CV9 - CALC - US8',	'PESO1_I@08MO-BW-813I-04M1',	'PESO1_I@08PE-BW-840I-01M1',	'PESO1_I@08PE-BW-840I-02M1',	'PESO1_I@08PE-BW-840I-03M1',	'PESO1_I@08PE-BW-840I-04M1',	'PESO1_I@08PE-BW-840I-05M1',	'PESO1_I@08PE-BW-840I-06M1',	'PESO1_I@08PE-BW-840I-07M1',	'PESO1_I@08PE-BW-840I-08M1',	'PESO1_I@08PE-BW-840I-09M1',	'PESO1_I@08PE-BW-840I-10M1',	'PESO1_I@08PE-BW-840I-11M1',	'PESO1_I@08PE-BW-840I-12M1',	'PESO1_I@08PE-TR-840I-28M1',	'PESO1_I@08PE-TR-840I-29M1',	'PESO1_I@08PE-TR-851I-03M1',	'PESO2_I@08MO-BW-813I-03M1',	'PESO2_I@08MO-BW-813I-04M1',	'POT TOTAL VENT - US8',	'POTE1_I@08QU-PF-852I-01M1',	'POTE1_I@08QU-PF-852I-02M1',	'POTE1_I@08QU-PF-852I-03M1',	'POTE1_I@08QU-PF-852I-04M1',	'POTE1_I@08QU-PF-852I-05M1',	'POTE1_I@08QU-PF-852I-06M1',	'POTE1_I@08QU-PF-852I-07M1',	'POTE1_I@08QU-PF-852I-08M1',	'PRES1_I@08QU-DU-851I-01',	'PRES1_I@08QU-WB-851I-21',	'PRES1_I@08QU-WB-851I-27',	'PRES1_I@08QU-WB-851I-31',	'PRES1_OCS_S@08QU-PF-852I-04M1',	'PROD_PC_I@08US',	'PROD1_OCS_D@08PE-BW-840I',	'PROD1_OCS_S@08PE-BW-840I',	'PV TEMP GQ3-16-MED - US8',	'qtde_discos',	'QUIM_CARVAO_PP_L@08PR',	'RETO1_Y@08PE',	'ROTA1_I@08QU-PF-852I-01M1',	'ROTA1_I@08QU-PF-852I-02M1',	'ROTA1_I@08QU-PF-852I-03M1',	'ROTA1_I@08QU-PF-852I-04M1',	'ROTA1_I@08QU-PF-852I-05M1',	'ROTA1_I@08QU-PF-852I-06M1',	'ROTA1_I@08QU-PF-852I-07M1',	'ROTA1_I@08QU-PF-852I-08M1',	'SIO2_OCS_I@08MO-CALCARIO',	'SOMA FUNC FILTROS',	'TEMP1_I@08QU-PP-871I-01',	'TEMP1_I@08QU-PP-871I-02',	'TEMP1_I@08QU-PP-871I-03',	'TEMP2_I@08QU-HO-851I-01',	'TEMP2_I@08QU-PP-871I-03',	'TEMP3_I@08QU-HO-851I-01',	'TEMP5_I@08QU-PF-852I-01M1',	'TEMP5_I@08QU-PF-852I-02M1',	'TEMP5_I@08QU-PF-852I-03M1',	'TEMP5_I@08QU-PF-852I-04M1',	'TEMP5_I@08QU-PF-852I-05M1',	'TEMP5_I@08QU-PF-852I-07M1',	'TEMP5_I@08QU-PF-852I-08M1',	'VAZA1_I@08QU-ST-855I-01',	'VELO1_C@08QU-FR-851I-01M1']
tags_compressao_abrasao.extend(gran_ocs_remover)

datasets['abrasao'].drop(columns = tags_compressao_abrasao, axis=1, inplace = True,errors="ignore")
datasets['compressao'].drop(columns = tags_compressao_abrasao, axis=1, inplace = True,errors="ignore")

add_model_tags(datasets, df_sql, 'abrasao', [ 'soma balanca minerio misturador','soma balanca bentonita misturador', 'soma balanca retorno correia','media temp pelotas','media disco de pelotamento', 'media tm'])

#['PEVT_PF852I05_M1_PIT_OCS_CDV_09a12', 'PRES1_C@08QU-PF-852I-04M1', 'PRES1_I@08QU-DU-853I-12', 'PRES2_C@08QU-PF-852I-03M1'] Não encontradas no dataset

add_model_tags(datasets, df_sql, 'compressao', [ 'soma balanca minerio misturador','soma balanca bentonita misturador', 'soma balanca retorno correia','media temp pelotas','media disco de pelotamento', 'media tm'])

#['PRES1_C@08QU-PF-852I-04M1', 'PRES1_I@08QU-DU-853I-12', 'PRES2_C@08QU-PF-852I-03M1'] Não encontradas no dataset


# MAGIC %md
# MAGIC ## Basicidade


#Adicionar as tags de balança

add_model_tags(datasets, df_sql, 'basicidade', [ 'SIO2_OCS_I@08MO-BENTONITA','SIO2_OCS_I@08MO-ANTRACITO', 'SIO2_OCS_I@08MO-CALCARIO', 'PESO1_I@08MO-BW-813I-01M1', 'vazao_antracito'])


# MAGIC %md
# MAGIC ## Cfix


#print(datasets['cfix'].columns)


add_model_tags(datasets, df_sql, 'cfix', ['FUNC1_D@08AD-BR-813I-01M1',	'FUNC1_D@08AD-BR-813I-02M1',	'ROTA1_I@08AD-BR-813I-01M1','ROTA1_I@08AD-BR-813I-02M1',	'FUNC1_D@08PA-TR-811I-08M1',	'NIVE1_I@08HO-TQ-826I-03',	'NIVE1_I@08HO-TQ-826I-04','NIVE1_I@08HO-TQ-826I-05', 'vazao_antracito'])

#'PEVT_US8_MOA_CVMI_AN_AIT_LAB_P_FG_-0,045MM' -- só possui no PI System 

# ['DESV1_I@08PA-TR-811I-08M1', 'DESV1_I@08MO-BW-813I-03M1', 'DESV1_I@08MO-BW-813I-04M1', 'PEVT_US8_MOA_CVMI_AN_AIT_LAB_P_FG_-0,045MM'] Não encontradas no dataset


# MAGIC %md
# MAGIC ## Densidade Moinho 1,2 e 3


# #Verificação se já consta no modelo 
# 'vazao_antracito' in 'dens_moinho_1'


# *bomba de retorno do tanque/(bomba retorno do tanque+vazão de saída do tanque): verificar correlação para possibilidade


add_model_tags(datasets, df_sql, 'dens_moinho_1', ['PESO1_I@08MO-BW-813I-04M1', 'PESO1_I@08MO-BW-813I-03M1', 'PESO2_I@08MO-BW-813I-03M1', 'PESO2_I@08MO-BW-813I-04M1', 'VAZA1_I@08AP-BP-875I-01','DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-01','VAZA1_I@08MO-MO-821I-01',  'VAZA1_I@08MO-BP-821I-01M1', 'VAZA1_I@08MO-TQ-821I-01', 'VAZA1_I@08MO-TQ-821I-01', 'vazao_antracito', 'bomba de retorno tanque'])

datasets['dens_moinho_1'].drop(columns = ['antracito','NIVE1_I@08MO-SI-813I-06','PESO2_Q@08MO-TR-821I-02M1-MO01','POTE1_I@08MO-MO-821I-01M1','Consumo de Energia (base minério úmido) kWh/ton','FUNC1_D@08AD-BR-813I-01M1','FUNC1_D@08AD-BR-813I-02M1','PESO1_I@08MO-BW-813I-01M1','NIVE1_I@08MO-TQ-821I-01','ROTA1_I@08AD-BR-813I-02M1','ROTA1_I@08AD-BR-813I-01M1','corpo_moedor_especifico',	'CARV1_OCS_S@08PP',	'ProducaoPQ_Moagem','status','qtde_moinhos','SUP_SE_PR_L@08FI',	'Umidade minério de alimentação', 'Consumo de Energia (base minério úmido) kWh/ton'], axis=1, inplace=True, errors="ignore")


add_model_tags(datasets, df_sql, 'dens_moinho_2', ['bomba de retorno tanque'])

datasets['dens_moinho_2'].drop(columns = ['antracito', 'Consumo de Energia (base minério úmido) kWh/ton','corpo_moedor_especifico','FUNC1_D@08AD-BR-813I-01M1' ,'FUNC1_D@08AD-BR-813I-02M1', 'PESO1_I@08MO-BW-813I-01M1', 'ROTA1_I@08AD-BR-813I-01M1', 'ROTA1_I@08AD-BR-813I-02M1','status', 'SUP_SE_PR_L@08FI'], axis=1, inplace=True, errors="ignore")


add_model_tags(datasets, df_sql, 'dens_moinho_3', ['bomba de retorno tanque'])

datasets['dens_moinho_3'].drop(columns = ['antracito', 'CARV1_OCS_S@08PP', 'Consumo de Energia (base minério úmido) kWh/ton', 'corpo_moedor_especifico', 'FUNC1_D@08AD-BR-813I-01M1','FUNC1_D@08AD-BR-813I-02M1', 'PESO1_I@08MO-BW-813I-01M1', 'ROTA1_I@08AD-BR-813I-01M1', 'ROTA1_I@08AD-BR-813I-02M1', 'status','SUP_SE_PR_L@08FI' ], axis=1, inplace=True, errors="ignore")


# MAGIC %md
# MAGIC ## Modelos de Forno 
# MAGIC energia_forno, energia_moinho e energia_prensa


#energia_forno
#está utilizando apenas a potencia 
#CONS ESP VENT TOTAL - US8 - target

datasets['energia_forno'].drop(columns = ['PROD_PQ_Y@08US', 'status'], axis=1, inplace=True, errors="ignore")


#energia_moinho

datasets['energia_forno'].drop(columns = ['ProducaoPQ_Moagem', 'status'], axis=1, inplace=True, errors="ignore")


#energia_prensa

datasets['energia_prensa'].drop(columns = ['CONS1_Y@08QU-PF-852I-01M1', 'CONS1_Y@08QU-PF-852I-02M1','CONS1_Y@08QU-PF-852I-03M1','CONS1_Y@08QU-PF-852I-04M1','CONS1_Y@08QU-PF-852I-05M1','CONS1_Y@08QU-PF-852I-06M1','CONS1_Y@08QU-PF-852I-07M1','CONS1_Y@08QU-PF-852I-08M1'], axis=1, inplace=True, errors="ignore")


# MAGIC %md
# MAGIC ## Finos


datasets.pop('finos', None)
print(datasets.keys())

datasets['finos'] = datasets['abrasao'].copy()

add_model_tags(datasets, df_sql, 'finos', ['GRAN_-5_PQ_L@08QU']) 


# MAGIC %md
# MAGIC ## Gas


#analisar o missing, pois os valores estão próximos no modelo todo


# MAGIC %md
# MAGIC ## GRAN PR


# #Exclusão do modelo 
# datasets.pop('GRAN_PR', None)

# datasets['GRAN PR'] = datasets['SE PR'].copy()


# #comentar	DENS1_C@08MO-MO-821I-01,DENS1_C@08MO-MO-821I-02,DENS1_C@08MO-MO-821I-03

# # "correlação com Energia elétrica Média" - POTE1_I@08MO-MO-821I-01M1, POTE1_I@08MO-MO-821I-02M1,POTE1_I@08MO-MO-821I-03M1

# datasets['GRAN PR'].drop(columns = ['antracito','CARV1_OCS_S@08PP','FUNC1_D@08AD-BR-813I-01M1',	'FUNC1_D@08AD-BR-813I-02M1',	'FUNC1_D@08MO-MO-821I-01M1',	'FUNC1_D@08MO-MO-821I-02M1',	'FUNC1_D@08MO-MO-821I-03M1',	'NIVE1_I@08MO-SI-813I-06',	'NIVE1_I@08MO-TQ-821I-01',	'NIVE1_I@08MO-TQ-821I-02',	'NIVE1_I@08MO-TQ-821I-03',	'PESO1_I@08MO-BW-813I-01M1',	'PESO1_I@08MO-BW-813I-03M1',	'PESO1_I@08MO-BW-813I-04M1',	'PESO2_I@08MO-BW-813I-03M1',	'PESO2_I@08MO-BW-813I-04M1',	'PESO2_Q@08MO-TR-821I-02M1-MO01',	'PESO2_Q@08MO-TR-821I-02M1-MO02',	'PESO2_Q@08MO-TR-821I-02M1-MO03',	'ROTA1_I@08AD-BR-813I-01M1',	'ROTA1_I@08AD-BR-813I-02M1', 'qtde_moinhos', 'PESO2_Q@08MO-TR-821I-02M1-MO01','PESO2_Q@08MO-TR-821I-02M1-MO02','PESO2_Q@08MO-TR-821I-02M1-MO03', 'calcario'], axis=1, inplace = True,errors="ignore")

# add_model_tags(datasets, df_sql, 'GRAN PR', ['media balanca moinho', 'media_vazao_antracito', 'media_potencia_moinho', 'bomba de retorno tanque', 'GRAN_-0,045_PR_L@08FI']) 


# datasets.pop('GRAN_PR', None)
# print(datasets.keys())

# datasets['GRAN PR'] = datasets['SE PR'].copy()


# #comentar	DENS1_C@08MO-MO-821I-01,DENS1_C@08MO-MO-821I-02,DENS1_C@08MO-MO-821I-03

# # "correlação com Energia elétrica Média" - POTE1_I@08MO-MO-821I-01M1, POTE1_I@08MO-MO-821I-02M1,POTE1_I@08MO-MO-821I-03M1

# datasets['GRAN PR'].drop(columns = ['antracito','CARV1_OCS_S@08PP','FUNC1_D@08AD-BR-813I-01M1',	'FUNC1_D@08AD-BR-813I-02M1',	'FUNC1_D@08MO-MO-821I-01M1',	'FUNC1_D@08MO-MO-821I-02M1',	'FUNC1_D@08MO-MO-821I-03M1',	'NIVE1_I@08MO-SI-813I-06',	'NIVE1_I@08MO-TQ-821I-01',	'NIVE1_I@08MO-TQ-821I-02',	'NIVE1_I@08MO-TQ-821I-03',	'PESO1_I@08MO-BW-813I-01M1',	'PESO1_I@08MO-BW-813I-03M1',	'PESO1_I@08MO-BW-813I-04M1',	'PESO2_I@08MO-BW-813I-03M1',	'PESO2_I@08MO-BW-813I-04M1',	'PESO2_Q@08MO-TR-821I-02M1-MO01',	'PESO2_Q@08MO-TR-821I-02M1-MO02',	'PESO2_Q@08MO-TR-821I-02M1-MO03',	'ROTA1_I@08AD-BR-813I-01M1',	'ROTA1_I@08AD-BR-813I-02M1', 'qtde_moinhos', 'PESO2_Q@08MO-TR-821I-02M1-MO01','PESO2_Q@08MO-TR-821I-02M1-MO02','PESO2_Q@08MO-TR-821I-02M1-MO03', 'calcario'], axis=1, inplace = True,errors="ignore")

# add_model_tags(datasets, df_sql, 'GRAN PR', ['media balanca moinho', 'media_vazao_antracito', 'media_potencia_moinho', 'bomba de retorno tanque', 'GRAN_-0,045_PR_L@08FI']) 


# MAGIC %md
# MAGIC ## Produtividade Filtragem  


add_model_tags(datasets, df_sql, 'produtividade filtragem', ['media de densidade', 'mediana de rotacao'])


# MAGIC %md
# MAGIC ## relacao gran


datasets.pop('relacao gran', None)
print(datasets.keys())

datasets['relacao gran'] = datasets['distribuicao gran'].copy()

# datasets['relacao gran'].drop(columns = gran_ocs_remover, axis=1, inplace = True,errors="ignore")

add_model_tags(datasets, df_sql, 'relacao gran', ['rel_gran'])


# MAGIC %md
# MAGIC ## Modelos de rota disco 1 - 12


# # rota_disco_1
# add_model_tags(datasets, df_sql, 'rota_disco_1', ['ROTA1_I@08PE-PN-840I-01M1','POSI1_I@08PE-BD-840I-01',
# 'PESO1_I@08PE-TR-840I-30M1', 'PESO1_I@08PE-TR-851I-03M1', 'PESO1_I@08PN-TR-860I-09M1', 'UMID_H2O_PR_L@08FI', 'GRAN_-0,045_PP_L@08PR', 'SUP_SE_PP_L@08PR', 'POSI1_I@08PE-BD-840I-01', 'ROTA1_I@08PE-PN-840I-01M1', 'ROTA1_I@08PE-PN-851I-01'])

# #['POSI1_D@08PE-BD-840I-01M1'] Não encontrada no dataset, verificar integração


# # rota_disco_2
# add_model_tags(datasets, df_sql, 'rota_disco_2', ['ROTA1_I@08PE-PN-840I-02M1','PESO1_I@08PE-TR-840I-30M1','PESO1_I@08PE-TR-851I-03M1', 'PESO1_I@08PN-TR-860I-09M1', 'UMID_H2O_PR_L@08FI', 'GRAN_-0,045_PP_L@08PR', 'SUP_SE_PP_L@08PR', 'PESO1_I@08PE-TR-840I-30M1','PESO1_I@08PE-TR-851I-03M1','PESO1_I@08PN-TR-860I-09M1','UMID_H2O_PR_L@08FI', 'GRAN_-0,045_PP_L@08PR', 'SUP_SE_PP_L@08PR', 'ROTA1_I@08PE-PN-840I-02M1','ROTA1_I@08PE-PN-851I-02-1','ROTA1_I@08PE-PN-851I-02-2', 'ROTA1_I@08PE-PN-840I-02M1'])

# #['POSI1_D@08PE-BD-840I-02M1', 'POSI1_I@08PE-BD-840I-02'] Não encontradas no dataset


# # rota_disco_3
# add_model_tags(datasets, df_sql, 'rota_disco_3', ['PESO1_I@08PE-TR-840I-30M1','PESO1_I@08PE-TR-851I-03M1','PESO1_I@08PN-TR-860I-09M1','UMID_H2O_PR_L@08FI','GRAN_-0,045_PP_L@08PR','SUP_SE_PP_L@08PR','ROTA1_I@08PE-PN-840I-03M1', 'POSI1_I@08PE-BD-840I-03','POSI1_I@08PE-BD-840I-03', 'ROTA1_I@08PE-PN-840I-03M1'])

# #['POSI1_D@08PE-BD-840I-03M1'] Não encontradas no dataset


# # rota_disco_4
# add_model_tags(datasets, df_sql, 'rota_disco_4', ['PESO1_I@08PE-TR-840I-30M1','PESO1_I@08PE-TR-851I-03M1','PESO1_I@08PN-TR-860I-09M1',
# 'UMID_H2O_PR_L@08FI','GRAN_-0,045_PP_L@08PR','SUP_SE_PP_L@08PR','ROTA1_I@08PE-PN-840I-04M1', 'ROTA1_I@08PE-PN-840I-04M1'])

# #['POSI1_D@08PE-BD-840I-04M1', 'POSI1_I@08PE-BD-840I-04'] Não encontradas no dataset


# # rota_disco_5
# add_model_tags(datasets, df_sql, 'rota_disco_5', [ 'PESO1_I@08PE-TR-840I-30M1', 'PESO1_I@08PE-TR-851I-03M1', 'PESO1_I@08PN-TR-860I-09M1', 'UMID_H2O_PR_L@08FI', 'GRAN_-0,045_PP_L@08PR', 'SUP_SE_PP_L@08PR','ROTA1_I@08PE-PN-840I-05M1','POSI1_I@08PE-BD-840I-05', 'ROTA1_I@08PE-PN-840I-05M1'])

# #['POSI1_D@08PE-BD-840I-05M1'] Não encontradas no dataset


# # rota_disco_6
# add_model_tags(datasets, df_sql, 'rota_disco_6', ['PESO1_I@08PE-TR-840I-30M1','PESO1_I@08PE-TR-851I-03M1','PESO1_I@08PN-TR-860I-09M1',
# 'UMID_H2O_PR_L@08FI','GRAN_-0,045_PP_L@08PR','SUP_SE_PP_L@08PR','ROTA1_I@08PE-PN-840I-06M1', 'ROTA1_I@08PE-PN-840I-06M1'])

# #['POSI1_D@08PE-BD-840I-06M1', 'POSI1_I@08PE-BD-840I-06'] Não encontradas no dataset


# # rota_disco_7 
# add_model_tags(datasets, df_sql, 'rota_disco_7', ['PESO1_I@08PE-TR-840I-30M1','PESO1_I@08PE-TR-851I-03M1','PESO1_I@08PN-TR-860I-09M1','UMID_H2O_PR_L@08FI','GRAN_-0,045_PP_L@08PR','SUP_SE_PP_L@08PR','ROTA1_I@08PE-PN-840I-07M1','POSI1_I@08PE-BD-840I-07', 'ROTA1_I@08PE-PN-840I-07M1'])

# #['POSI1_D@08PE-BD-840I-07M1'] Não encontradas no dataset


# # rota_disco_8
# add_model_tags(datasets, df_sql, 'rota_disco_8', ['PESO1_I@08PE-TR-840I-30M1','PESO1_I@08PE-TR-851I-03M1','PESO1_I@08PN-TR-860I-09M1',
# 'UMID_H2O_PR_L@08FI','GRAN_-0,045_PP_L@08PR','SUP_SE_PP_L@08PR','ROTA1_I@08PE-PN-840I-08M1',  'ROTA1_I@08PE-PN-840I-08M1'])


# #['POSI1_D@08PE-BD-840I-08M1', 'POSI1_I@08PE-BD-840I-08'] Não encontradas no dataset


# # rota_disco_9
# add_model_tags(datasets, df_sql, 'rota_disco_9', ['PESO1_I@08PE-TR-840I-30M1','PESO1_I@08PE-TR-851I-03M1','PESO1_I@08PN-TR-860I-09M1',
# 'UMID_H2O_PR_L@08FI','GRAN_-0,045_PP_L@08PR','SUP_SE_PP_L@08PR','ROTA1_I@08PE-PN-840I-09M1','POSI1_I@08PE-BD-840I-09', 'ROTA1_I@08PE-PN-840I-09M1'])

# # ['POSI1_D@08PE-BD-840I-09M1'] Não encontradas no dataset


# # rota_disco_10
# add_model_tags(datasets, df_sql, 'rota_disco_10', ['PESO1_I@08PE-TR-840I-30M1','PESO1_I@08PE-TR-851I-03M1','PESO1_I@08PN-TR-860I-09M1','UMID_H2O_PR_L@08FI','GRAN_-0,045_PP_L@08PR','SUP_SE_PP_L@08PR','ROTA1_I@08PE-PN-840I-10M1', 'ROTA1_I@08PE-PN-840I-10M1'])


# #['POSI1_D@08PE-BD-840I-10M1', 'POSI1_I@08PE-BD-840I-10'] Não encontradas no dataset


# # rota_disco_11
# add_model_tags(datasets, df_sql, 'rota_disco_11', ['PESO1_I@08PE-TR-840I-30M1','PESO1_I@08PE-TR-851I-03M1','PESO1_I@08PN-TR-860I-09M1','UMID_H2O_PR_L@08FI','GRAN_-0,045_PP_L@08PR','SUP_SE_PP_L@08PR','ROTA1_I@08PE-PN-840I-11M1', 'ROTA1_I@08PE-PN-840I-11M1'])

# #['POSI1_D@08PE-BD-840I-11M1'] Não encontradas no dataset


# # rota_disco_12
# add_model_tags(datasets, df_sql, 'rota_disco_12', ['PESO1_I@08PE-TR-840I-30M1','PESO1_I@08PE-TR-851I-03M1','PESO1_I@08PN-TR-860I-09M1','UMID_H2O_PR_L@08FI','GRAN_-0,045_PP_L@08PR','SUP_SE_PP_L@08PR','ROTA1_I@08PE-PN-840I-12M1',  'ROTA1_I@08PE-PN-840I-12M1'])


# #['POSI1_D@08PE-BD-840I-12M1', 'POSI1_I@08PE-BD-840I-12'] Não encontradas no dataset


# MAGIC %md
# MAGIC ## SE PP
# MAGIC demanda Aileen 2022-10-20


# 'ROTA1_I@08PR-RP-822I-01M1' in 'SE PP'


# # tags a serem analisadas NIVE1_I@08HO-TQ-826I-03, NIVE1_I@08HO-TQ-826I-04, NIVE1_I@08HO-TQ-826I-05
# #Escorregamento - realizar teste do modelo com e sem
# #trocar as tags de torque - as tags são as mesmas, análise já realizada 

# add_model_tags(datasets, df_sql, 'SE PP', ['UMID_H2O_PR_L@08FI', 'media vel de disco de pel', 'NIVE1_I@08HO-TQ-826I-03','NIVE1_I@08HO-TQ-826I-04','NIVE1_I@08HO-TQ-826I-05', 'media gap prensa','media rolo fixo prensa','media oleo prensa', 'Escorregamento'])
# #'diferenca oleo prensa'

# datasets['SE PP'].drop(['antracito','CALC1_Y@08FI-FD00','CALC1_Y@08FI-FD00-L1','CALC1_Y@08FI-FD00-L2','Calculo da Energia da Filtragem','DENS1_C@08HO-BP-826I-05','DENS1_C@08HO-BP-826I-06R','DENS1_C@08HO-BP-826I-07','DENS1_C@08HO-BP-826I-08R','FUNC1_D@08FI-FL-827I-01','FUNC1_D@08FI-FL-827I-02','FUNC1_D@08FI-FL-827I-03','FUNC1_D@08FI-FL-827I-04','FUNC1_D@08FI-FL-827I-05R','FUNC1_D@08FI-FL-827I-06',	'FUNC1_D@08FI-FL-827I-07',	'FUNC1_D@08FI-FL-827I-08',	'FUNC1_D@08FI-FL-827I-09',	'FUNC1_D@08FI-FL-827I-10R',	'GAP CALC PRENSA - US8','NIVE1_I@08MO-SI-813I-06','PESO1_I@08MO-BW-813I-03M1','PESO1_I@08MO-BW-813I-04M1','PESO2_I@08MO-BW-813I-03M1',	'PESO2_I@08MO-BW-813I-04M1','POTE1_I@08FI-BV-827I-01M1','POTE1_I@08FI-BV-827I-02M1','POTE1_I@08FI-BV-827I-03M1','POTE1_I@08FI-BV-827I-04M1','POTE1_I@08FI-BV-827I-05RM1','POTE1_I@08FI-BV-827I-06M1','POTE1_I@08FI-BV-827I-07M1','POTE1_I@08FI-BV-827I-08M1',	'POTE1_I@08FI-BV-827I-09M1','POTE1_I@08FI-BV-827I-10RM1','PRES1_I@08FI-BV-827I','PROD FILTR - US8','ROTA1_I@08FI-FL-827I-01M1',	'ROTA1_I@08FI-FL-827I-02M1','ROTA1_I@08FI-FL-827I-03M1','ROTA1_I@08FI-FL-827I-04M1','ROTA1_I@08FI-FL-827I-05RM1','ROTA1_I@08FI-FL-827I-06M1','ROTA1_I@08FI-FL-827I-07M1','ROTA1_I@08FI-FL-827I-08M1','ROTA1_I@08FI-FL-827I-09M1','ROTA1_I@08FI-FL-827I-10RM1',	'ROTA1_I@08HO-AG-826I-01M1','ROTA1_I@08HO-AG-826I-02M1','ROTA1_I@08HO-AG-826I-03M1','RPM MED FILTROS - US8','TORQ1_I@08PR-RP-822I-01M1', 'CONS EE PRENSA - US8', 'GANHO PRENSA - US8', 'GRAN_-0,045_PR_L@08FI', 'NIVE1_C@08PR-RP-822I-01','NIVE1_I@08HO-TQ-826I-03','NIVE1_I@08HO-TQ-826I-04','NIVE1_I@08HO-TQ-826I-05', 'QUIM_SIO2_MA_L@08PA', 'TORQ1_I@08PR-RP-822I-01M2'], axis=1, inplace=True, errors="ignore")


# MAGIC %md
# MAGIC ## SE PR


# #comentar	DENS1_C@08MO-MO-821I-01,DENS1_C@08MO-MO-821I-02,DENS1_C@08MO-MO-821I-03

# # "correlação com Energia elétrica Média" - POTE1_I@08MO-MO-821I-01M1, POTE1_I@08MO-MO-821I-02M1,POTE1_I@08MO-MO-821I-03M1

# datasets['SE PR'].drop(columns = ['antracito','CARV1_OCS_S@08PP','FUNC1_D@08AD-BR-813I-01M1',	'FUNC1_D@08AD-BR-813I-02M1',	'FUNC1_D@08MO-MO-821I-01M1',	'FUNC1_D@08MO-MO-821I-02M1',	'FUNC1_D@08MO-MO-821I-03M1',	'NIVE1_I@08MO-SI-813I-06',	'NIVE1_I@08MO-TQ-821I-01',	'NIVE1_I@08MO-TQ-821I-02',	'NIVE1_I@08MO-TQ-821I-03',	'PESO1_I@08MO-BW-813I-01M1',	'PESO1_I@08MO-BW-813I-03M1',	'PESO1_I@08MO-BW-813I-04M1',	'PESO2_I@08MO-BW-813I-03M1',	'PESO2_I@08MO-BW-813I-04M1',	'PESO2_Q@08MO-TR-821I-02M1-MO01',	'PESO2_Q@08MO-TR-821I-02M1-MO02',	'PESO2_Q@08MO-TR-821I-02M1-MO03',	'ROTA1_I@08AD-BR-813I-01M1',	'ROTA1_I@08AD-BR-813I-02M1', 'qtde_moinhos', 'PESO2_Q@08MO-TR-821I-02M1-MO01','PESO2_Q@08MO-TR-821I-02M1-MO02','PESO2_Q@08MO-TR-821I-02M1-MO03', 'calcario'], axis=1, inplace = True,errors="ignore")

# add_model_tags(datasets, df_sql, 'SE PR', ['media balanca moinho', 'media_vazao_antracito', 'media_potencia_moinho', 'bomba de retorno tanque']) 


# MAGIC %md
# MAGIC ## taxarp


#RETO1_Y@08PE - verificar essa tag com mais calma

# realizar média, testar com ou sem - 'media disco de pelotamento']

#realizar média, testar com ou sem - 'media alimentacao do disco'

add_model_tags(datasets, df_sql, 'taxarp', ['media disco de pelotamento', 'media alimentacao do disco', 'media tm'])


# MAGIC %md
# MAGIC ## temp_forno


#analisar na quarta


# MAGIC %md
# MAGIC ## temp_recirc


#não precisa realizar nenhuma modificação por enquanto 


# MAGIC %md
# MAGIC ## torque


#Aileen rever modelo de torque com gilvandro 


# MAGIC %md
# MAGIC ## umidade 


add_model_tags(datasets, df_sql, 'produtividade filtragem', ['media de densidade', 'mediana de rotacao'])


# MAGIC %md
# MAGIC ## remocoes de modelos


modelos_remover = []
modelos_remover.extend(['temp_precipitador_1', 'temp_precipitador_2', 'temp_precipitador_3'])
modelos_remover.extend(['particulados1', 'particulados2', 'particulados3'])
modelos_remover.extend(['custo_abrasao', 'custo_compressao', 'custo_distribuicao gran', 'custo_SE PP', 'custo_GRAN PR', 'custo_SE PR','distribuicao gran'])


for model in modelos_remover:
    datasets.pop(model, None)
print(datasets.keys())


# MAGIC %md
# MAGIC ## Target
# MAGIC 
# MAGIC ###### Definindo o target do modelo 


modelos_rota = ['GRAN PR', 'relacao gran','rota_disco_1','rota_disco_2','rota_disco_3','rota_disco_4','rota_disco_5','rota_disco_6','rota_disco_7','rota_disco_8','rota_disco_9','rota_disco_10','rota_disco_11','rota_disco_12', 'finos']

targets_rota = ['GRAN_-0,045_PR_L@08FI', 'rel_gran', 'GRAN_OCS_TM@08PE-BD-840I-01', 'GRAN_OCS_TM@08PE-BD-840I-02', 'GRAN_OCS_TM@08PE-BD-840I-03', 'GRAN_OCS_TM@08PE-BD-840I-04', 'GRAN_OCS_TM@08PE-BD-840I-05', 'GRAN_OCS_TM@08PE-BD-840I-06', 'GRAN_OCS_TM@08PE-BD-840I-07', 'GRAN_OCS_TM@08PE-BD-840I-08', 'GRAN_OCS_TM@08PE-BD-840I-09', 'GRAN_OCS_TM@08PE-BD-840I-10', 'GRAN_OCS_TM@08PE-BD-840I-11', 'GRAN_OCS_TM@08PE-BD-840I-12', 'GRAN_-5_PQ_L@08QU']

assert(len(modelos_rota) == len(targets_rota))

for model, target in list(zip(modelos_rota, targets_rota)):
    if model.startswith('rota'): continue
    cols = datasets[model].columns.to_list()
    cols.remove(target)
    datasets[model] = datasets[model][cols+[target]]


# MAGIC %md
# MAGIC # rename bad variable names


bad_names = {'%CF. ANT': '%CF_ANT'}
df_sql.rename(bad_names, axis=1, inplace=True)
for model, df in datasets.items():
    datasets[model] = df.rename(bad_names, axis=1)


# MAGIC %md
# MAGIC #save files


data_path = '/dbfs/tmp/us8'
if not(os.path.exists(data_path)):
        os.mkdir(data_path)

data_file = 'datasets.joblib'
df_sql_file = 'df_sql.joblib'
joblib.dump(datasets, os.path.join(data_path, data_file))
joblib.dump(df_sql, os.path.join(data_path, df_sql_file))
joblib.dump(df_sql_raw, os.path.join(data_path, 'df_sql_raw.joblib'))

dbutils.notebook.exit([data_path, data_file, df_sql_file])


# MAGIC %md
# MAGIC # checkup


for model, df in datasets.items():
    for c in df.columns:
        if c.startswith('GRAN'):
            print(model, c)

# for c in df_train.columns:
#     print(qualidade, c)
#     limits[c] = df_train[c] 


datasets['relacao gran']['GRAN_OCS_5-8@08PE-BD-840I-12'].describe()


# MAGIC %md
# MAGIC # checa zeros e nulos


null_tags = teste[teste['Nulos']>50].shape[0]
zero_tags = teste[teste['Zeros']>50].shape[0]
log.info('treshhold 50%')
log.info(f'total number of tags: {teste.shape[0]}')
log.info(f'null_tags: {null_tags}, null_tags perc: {round(null_tags / teste.shape[0] * 100, 2)}')
log.info(f'zero_tags: {zero_tags}, zero_tags perc: {round(zero_tags / teste.shape[0] * 100, 2)}')


null_tags = teste[teste['Nulos']>90].shape[0]
zero_tags = teste[teste['Zeros']>90].shape[0]
print()
log.info('treshhold 90%')
log.info(f'total number of tags: {teste.shape[0]}')
log.info(f'null_tags: {null_tags}, null_tags perc: {round(null_tags / teste.shape[0] * 100, 2)}')
log.info(f'zero_tags: {zero_tags}, zero_tags perc: {round(zero_tags / teste.shape[0] * 100, 2)}')

null_tags = teste[teste['Nulos']>99].shape[0]
zero_tags = teste[teste['Zeros']>99].shape[0]
print()
log.info('treshhold 99%')
log.info(f'total number of tags: {teste.shape[0]}')
log.info(f'null_tags: {null_tags}, null_tags perc: {round(null_tags / teste.shape[0] * 100, 2)}')
log.info(f'zero_tags: {zero_tags}, zero_tags perc: {round(zero_tags / teste.shape[0] * 100, 2)}')


path_save = f'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/Usina08/Otimizacao/analises/'
save_csv(path_save, 'tag_zer_nulo.csv', teste, dbutils, sep=',')


# MAGIC %md
# MAGIC ### analisa por periodo
# MAGIC * 2021 vs. 2022


null_tags = teste_2021[teste_2021['Nulos']>50].shape[0]
zero_tags = teste_2021[teste_2021['Zeros']>50].shape[0]
log.info('treshhold 50%')
log.info(f'total number of tags: {teste_2021.shape[0]}')
log.info(f'null_tags: {null_tags}, null_tags perc: {round(null_tags / teste_2021.shape[0] * 100, 2)}')
log.info(f'zero_tags: {zero_tags}, zero_tags perc: {round(zero_tags / teste_2021.shape[0] * 100, 2)}')


null_tags = teste_2021[teste_2021['Nulos']>90].shape[0]
zero_tags = teste_2021[teste_2021['Zeros']>90].shape[0]
print()
log.info('treshhold 90%')
log.info(f'total number of tags: {teste_2021.shape[0]}')
log.info(f'null_tags: {null_tags}, null_tags perc: {round(null_tags / teste_2021.shape[0] * 100, 2)}')
log.info(f'zero_tags: {zero_tags}, zero_tags perc: {round(zero_tags / teste_2021.shape[0] * 100, 2)}')

null_tags = teste_2021[teste_2021['Nulos']>99].shape[0]
zero_tags = teste_2021[teste_2021['Zeros']>99].shape[0]
print()
log.info('treshhold 99%')
log.info(f'total number of tags: {teste_2021.shape[0]}')
log.info(f'null_tags: {null_tags}, null_tags perc: {round(null_tags / teste_2021.shape[0] * 100, 2)}')
log.info(f'zero_tags: {zero_tags}, zero_tags perc: {round(zero_tags / teste_2021.shape[0] * 100, 2)}')


null_tags = teste_2022[teste_2022['Nulos']>50].shape[0]
zero_tags = teste_2022[teste_2022['Zeros']>50].shape[0]
log.info('treshhold 50%')
log.info(f'total number of tags: {teste_2022.shape[0]}')
log.info(f'null_tags: {null_tags}, null_tags perc: {round(null_tags / teste_2022.shape[0] * 100, 2)}')
log.info(f'zero_tags: {zero_tags}, zero_tags perc: {round(zero_tags / teste_2022.shape[0] * 100, 2)}')


null_tags = teste_2022[teste_2022['Nulos']>90].shape[0]
zero_tags = teste_2022[teste_2022['Zeros']>90].shape[0]
print()
log.info('treshhold 90%')
log.info(f'total number of tags: {teste_2022.shape[0]}')
log.info(f'null_tags: {null_tags}, null_tags perc: {round(null_tags / teste_2022.shape[0] * 100, 2)}')
log.info(f'zero_tags: {zero_tags}, zero_tags perc: {round(zero_tags / teste_2022.shape[0] * 100, 2)}')

null_tags = teste_2022[teste_2022['Nulos']>99].shape[0]
zero_tags = teste_2022[teste_2022['Zeros']>99].shape[0]
print()
log.info('treshhold 99%')
log.info(f'total number of tags: {teste_2022.shape[0]}')
log.info(f'null_tags: {null_tags}, null_tags perc: {round(null_tags / teste_2022.shape[0] * 100, 2)}')
log.info(f'zero_tags: {zero_tags}, zero_tags perc: {round(zero_tags / teste_2022.shape[0] * 100, 2)}')


# MAGIC %md
# MAGIC # draft


# MAGIC %md
# MAGIC # filtra ou ajusta hora em dados de laboratorio (somente 4 em 4 horas)
# MAGIC precisa escolher uma das estratégias
# MAGIC * a 1a nao faz mais sentido quando vimos que os dados que saem de laboratório advem de coleta das ;ultimas 4 horas, formando uma médias dessas.


# MAGIC %md
# MAGIC ## filtro


# for model in ['abrasao', 'compressao']:
#     datasets[model] = datasets[model][datasets[model].index.hour.isin([3,7,11,15,19,23])]


# MAGIC %md
# MAGIC ## ajuste


# for model in ['abrasao', 'compressao']:
#     target = datasets[model].columns[-1]
#     dates = list(datasets[model].index[:])
#     values = list(datasets[model][target].values[:])
#     for count, date in enumerate(dates):
#         i = date.hour
#         resto = (i+1) % 4
#         if resto == 0:
#             continue
#         elif resto == 1:
#             hour_lab = i+3
#         elif resto == 2:
#             hour_lab = i+2
#         elif resto == 3:
#             hour_lab = i+1
#         date_lab = date.replace(hour=hour_lab)
#         try:
#             new_value = values[dates.index(date_lab)]
#         except:
#             new_value = False
#         values[count] = new_value
#     datasets[model][target] = values
#     #filter
#     datasets[model] = datasets[model][~datasets[model][target].isna()]
#     datasets[model] = datasets[model][datasets[model][target] != False]


