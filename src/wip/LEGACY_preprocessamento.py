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


us_sufix = '08'
solver_path = f'us{us_sufix}'.replace('0', '')


sys.path.append(os.path.join('/Workspace/Repos/c0648003@vale.com/temp_repo/src'))
sys.path.append(os.path.join('/Workspace/Repos/c0648003@vale.com/temp_repo', 'src', f'{solver_path}'))
sys.path.append(os.path.join(f'/Workspace/Repos/c0648003@vale.com/temp_repo', 'src', f'{solver_path}', 'variables'))

sys.path.append(os.path.join('/Workspace/Repos/c0621375@vale.com/pelletizing-otimizacao-processos/src'))
sys.path.append(os.path.join('/Workspace/Repos/c0621375@vale.com/pelletizing-otimizacao-processos', 'src', f'{solver_path}'))
sys.path.append(os.path.join('/Workspace/Repos/c0621375@vale.com/pelletizing-otimizacao-processos', 'src', f'{solver_path}', 'variables'))

sys.path.append(os.path.join('/Workspace/Repos/c0646872@vale.com/pelletizing-otimizacao-processos/src'))
sys.path.append(os.path.join('/Workspace/Repos/c0646872@vale.com/pelletizing-otimizacao-processos', 'src', f'{solver_path}'))
sys.path.append(os.path.join('/Workspace/Repos/c0646872@vale.com/pelletizing-otimizacao-processos', 'src', f'{solver_path}', 'variables'))


import us8.predictive_module
import model_module
import utils
import sql_module
# import analises.analise_database

importlib.reload(us8.predictive_module)
importlib.reload(model_module)
importlib.reload(utils)
importlib.reload(sql_module)
# importlib.reload(analises.analise_database)

from utils import log, add_model_tags, save_csv
from sql_module import sqlModule
# from analises.analise_database import *


import process_tag_map
import datasets_limits
import datasets_confs
import tags

importlib.reload(process_tag_map)
importlib.reload(datasets_limits)
importlib.reload(datasets_confs)
importlib.reload(tags)

from process_tag_map import process_tag_map
from datasets_limits import limits
from datasets_confs import datasets_confs
from tags import *


#%run ./files/tags


# MAGIC %md
# MAGIC # continua


# # DICIONARIO MODELOS: so é importante se for rodar compressao_model e seus equivalentes
# listaModelos = ['Carbono Fixo', 'Umidade', 'Taxa de Retorno', 'Abrasão', 'Compressao']
# listaIDModelo = list(range(1,6))
# dicModelos = dict(zip(listaModelos, listaIDModelo))
# log.info(dicModelos)

# # DICIONARIO Usinas
# listaUsinas = ['Usina 01', 'Usina 02', 'Usina 03', 'Usina 04', 'Usina 05', 'Usina 06', 'Usina 07', 'Usina 08']
# listaIDUsinas = list(range(1,9))
# dicUsinas = dict(zip(listaUsinas, listaIDUsinas))
# log.info(dicUsinas)


n_usina = 8
# plotDebug = True

# LER FAIXA
faixamin = 700
faixamax = 1000
df_faixa = pd.DataFrame({'faixaMin':list(range(faixamin, faixamax, 50)), 'faixaMax':list(range(faixamin + 50, faixamax + 50, 50))})


tagpims = tagpims + tagOTM + taglab + tag_calc
old_len = len(tagpims)
tagpims = list(set(tagpims))
if old_len != len(tagpims): log.info('there are duplicates')


tagdl = sqlModule(dbutils).tagPIMStoDataLake(tagpims)
taglist = pd.DataFrame({'tagdl': tagdl, 'tagpims': tagpims})


# dicTags = dict(zip(taglist.tagdl, taglist.tagpims))


# MAGIC %md
# MAGIC # define tini e tfim


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


log.info(f'command started at: {datetime.now(tz)}')

# pode dar erro no pyodbc
df_sql = sqlModule(dbutils).getFVarParam(n_usina, tini_str, tfim, taglist, param="US8")


df_sql_grouped = df_sql.groupby(['variavel']).count().reset_index()
df_sql_grouped[df_sql_grouped['variavel'].str.startswith('GRAN_OCS_10-16')]


df_sql = df_sql.drop_duplicates(subset=['data', 'variavel'], keep="first")
df_sql.shape


df_sql.groupby(['data', 'variavel']).count()


df_sql = df_sql.pivot(index = 'data', columns = 'variavel', values = 'valor')


# o que vem do sql
len(df_sql.columns)


# o que deveria vir do sql
len(tagpims)


# o que deveria vir que não veio
len(set(tagpims) - set(df_sql.columns))


falta = list(set(tagpims) - set(df_sql.columns))


list(set(falta) - set(tag_calc))


# MAGIC %md
# MAGIC ### salva dados para checagem de zeros e nulos


teste = get_nulls_zeros(df_sql)


# df_sql_2021 = df_sql[df_sql.index < datetime(2022, 1,1)]
# df_sql_2022 = df_sql[df_sql.index >= datetime(2022, 1,1)]

# teste_2021 = get_nulls_zeros(df_sql_2021)
# teste_2022 = get_nulls_zeros(df_sql_2022)


# df_sql.isna().sum().reset_index().sort_values(by = [0], ascending=False).head(10)


# df_sql.index.min(), df_sql.index.max()


# MAGIC %md
# MAGIC ### fim do teste de nulos


# log.info(f'command started at: {datetime.now()}')

df_sql = sqlModule(dbutils, spark).getTAGsDelay(df_sql, tini, tfim, tagpims, taglist)
df_sql_raw = df_sql.copy()
df_usina8 = df_sql.copy()


# MAGIC %md
# MAGIC # add restriçoes


# df_sql = df_sql[df_sql.index <= '2020-11-01 23:00:00']
df_sql = df_sql[df_sql['PROD_PQ_Y@08US'] > 0]


### HotFix ate que a TAG POTE1_I@08FI-BV-827I-07M1 esteja no datalake - comentado - tag ja esta no datalake 20220524
### mesmo hotfix feito no notebook de modelUmidade
# df_sql['POTE1_I@08FI-BV-827I-07M1'] = df_sql['POTE1_I@08FI-BV-827I-06M1'].copy()


# add gabi
# Pressão de vácuo
for t in df_usina8.loc[:, ['PRES1_I@08FI-FL-827I-01', 'PRES1_I@08FI-FL-827I-02', 'PRES1_I@08FI-FL-827I-03', 'PRES1_I@08FI-FL-827I-04', 'PRES1_I@08FI-FL-827I-05R', 'PRES1_I@08FI-FL-827I-06',
           'PRES1_I@08FI-FL-827I-07', 'PRES1_I@08FI-FL-827I-08', 'PRES1_I@08FI-FL-827I-09', 'PRES1_I@08FI-FL-827I-10R']]:
    df_usina8.loc[df_usina8[t] > -.8, t] = np.nan


# teste para restriçao
df_sql[['PRES1_I@08FI-FL-827I-01', 'PRES1_I@08FI-FL-827I-02', 'PRES1_I@08FI-FL-827I-03', 'PRES1_I@08FI-FL-827I-04', 'PRES1_I@08FI-FL-827I-05R', 'PRES1_I@08FI-FL-827I-06',
           'PRES1_I@08FI-FL-827I-07', 'PRES1_I@08FI-FL-827I-08', 'PRES1_I@08FI-FL-827I-09', 'PRES1_I@08FI-FL-827I-10R']].describe()


df_sql = df_sql.drop_duplicates(keep=False)
df_sql = df_sql.replace([-np.inf, np.inf], np.nan)


for t in df_sql.loc[:, [x for x in df_sql.columns if 'ROTA1_I@08FI-FL-827I-' in x]]:
    df_sql.loc[((df_sql[t] < 0.8) | (df_sql[t] > 1)), t] = np.nan
    
df_sql = df_sql.replace([np.inf, -np.inf], np.nan).fillna(method='bfill').fillna(method='ffill').fillna(method='bfill').fillna(0)


# MAGIC %md
# MAGIC #calculadas


''# 1
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
df_sql['SOMA FUNC DISCOS'] = df_sql['FUNC1_D@08FI-FL-827I-01'] + df_sql['FUNC1_D@08FI-FL-827I-02'] + df_sql['FUNC1_D@08FI-FL-827I-03'] + df_sql['FUNC1_D@08FI-FL-827I-04'] + df_sql['FUNC1_D@08FI-FL-827I-05R'] + df_sql['FUNC1_D@08FI-FL-827I-06'] + df_sql['FUNC1_D@08FI-FL-827I-07'] + df_sql['FUNC1_D@08FI-FL-827I-08'] + df_sql['FUNC1_D@08FI-FL-827I-09'] + df_sql['FUNC1_D@08FI-FL-827I-10R']

# 59
p=df_sql['SOMA FUNC DISCOS']>0
df_sql['Produtividade Pelotamento Virtual'] = (df_sql['PROD_PC_I@08US'][p]-df_sql['PESO1_I@08PE-TR-851I-03M1'][p])/((7.5/2)*math.pi*df_sql['SOMA FUNC DISCOS'][p])

# 60
p=df_sql['SOMA FUNC DISCOS']>0
df_sql['Produtividade Pel Efetiva'] = (df_sql['PROD_PQ_Y@08US'][p])/(((7.5/2)*math.pi)*df_sql['SOMA FUNC DISCOS'][p])

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


dfs.keys()


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


# datasets_confs_path = 'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/Usina08/Otimizacao/files/'
# datasets_confs = load_adls_json(datasets_confs_path, 'datasets_confs.json')

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
            log.info(dfs[conf[0]].columns)
            log.info(conf[1])
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
            ts_torque = ['TORQ1_I@08PR-RP-822I-01M2','TORQ1_I@08PR-RP-822I-01M1']
            datasets[qual]['TORQ1'] = datasets[qual][ts_torque].mean(axis=1)
#             datasets[qual] = datasets[qual].drop(ts_torque, axis=1)
        
        if qual == 'basicidade':
            #tmp = ['QUIM_CAO_PP_L@08PR','QUIM_SIO2_MA_L@08PA','SFBR','P DP','IM','PPC Pilha','GRAN MA','Magnetita','FCAL','QUIM_CINZA_ANM_L@CVMI_2','IOCJ','Hematita microcristalina','MGO MED','QUIM_CARVAO_PP_L@08PR','QUIM_CFIX_PP_L@08PR','SIO2_OCS_I@08MO-CALCARIO','PXTO','minerio_CA','Quartzo','Hematita compacta','minerio_CN','AL2O3 MED','Martita','QUIM_MVOL_ANM_L@CVMI_2','MN MED','SIO2 DP','Hemtatita granular','Hematita lobular','QUIM_CAO_MA_L@08PA DP','minerio_CE','Minerais porosos','TIO2 DP','Goethita','TIO2 MED','AL2O3 DP','Hematita tabular','minerio_BR','MGO DP','Outros','MN DP']            
            tmp = ['QUIM_CAO_PP_L@08PR','QUIM_SIO2_MA_L@08PA','SFBR','P DP','PPC Pilha','GRAN MA','MGO MED','QUIM_CARVAO_PP_L@08PR','QUIM_CFIX_PP_L@08PR','SIO2_OCS_I@08MO-CALCARIO','minerio_CA','minerio_CN','AL2O3 MED','MN MED','SIO2 DP','QUIM_CAO_MA_L@08PA DP','minerio_CE','TIO2 DP','TIO2 MED','AL2O3 DP','minerio_BR','MGO DP','MN DP']            
            tmp += ['status']
            tmp = [x for x in datasets[qual].columns if x in tmp]
            datasets[qual] = datasets[qual][tmp]
            #datasets[qual] = datasets[qual].drop(['QUIM_CAO_PP_L@08PR','P DP','Magnetita','Hematita microcristalina','Quartzo','Hematita compacta','Martita','SIO2 DP','Hemtatita granular','Hematita lobular','QUIM_CAO_MA_L@08PA DP','Minerais porosos','TIO2 DP','Goethita','AL2O3 DP','Hematita tabular','MGO DP','Outros','MN DP'], axis=1)
#             datasets[qual] = datasets[qual].drop(['QUIM_CAO_PP_L@08PR','P DP','SIO2 DP','QUIM_CAO_MA_L@08PA DP','TIO2 DP','AL2O3 DP','MGO DP','MN DP'], axis=1)
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


datasets['temp_forno'] = datasets['abrasao'][['TEMP1_I@08PN-TR-860I-01', 'TEMP1_I@08PN-TR-860I-02']].copy()
datasets['temp_forno']['Media_temp'] = df_sql['Media_temp']
datasets['temp_precipitador'] = datasets['abrasao'][['TEMP1_I@08QU-PP-871I-01', 'TEMP1_I@08QU-PP-871I-02', 'TEMP1_I@08QU-PP-871I-03']]


datasets['relacao gran'] = datasets['distribuicao gran'].copy()
df_sql['rel_gran'] = ((df_sql['GRAN_12,5_PQ_L@08QU']+df_sql['GRAN_16_PQ_L@08QU']) / (df_sql['GRAN_10_PQ_L@08QU']+df_sql['GRAN_8_PQ_L@08QU'])).fillna(method='bfill').fillna(method='ffill').fillna(0)
datasets['relacao gran'] = datasets['relacao gran'].join(df_sql['rel_gran']).copy()
datasets['relacao gran'] = datasets['relacao gran'][(df_sql['rel_gran'] >= 0.8) & (df_sql['rel_gran'] <= 1.2)]
datasets['relacao gran'].drop([0], axis=1, inplace=True)


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
# add_model_tags(datasets, df_usina8, 'dens_moinho_1', ['VAZA1_I@08MO-BP-821I-01M1', 'VAZA1_I@08AP-BP-875I-01', 'DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-01', 'VAZA1_I@08MO-MO-821I-01', 'VAZA1_I@08MO-TQ-821I-01', 'UMID_H2O_AM_L@08MI'])
# add_model_tags(datasets, df_usina8, 'dens_moinho_2', ['VAZA1_I@08MO-BP-821I-02M1', 'VAZA1_I@08AP-BP-875I-01', 'DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-02', 'VAZA1_I@08MO-MO-821I-02', 'VAZA1_I@08MO-TQ-821I-02'])
# add_model_tags(datasets, df_usina8, 'dens_moinho_3', ['VAZA1_I@08MO-BP-821I-03M1', 'VAZA1_I@08AP-BP-875I-01', 'DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-03', 'VAZA1_I@08MO-MO-821I-03', 'VAZA1_I@08MO-TQ-821I-03'])


add_model_tags(datasets, df_usina8, 'dens_moinho_1', ['VAZA1_I@08MO-BP-821I-01M1', 'VAZA1_I@08AP-BP-875I-01', 'DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-01', 'VAZA1_I@08MO-MO-821I-01', 'VAZA1_I@08MO-TQ-821I-01'])
add_model_tags(datasets, df_usina8, 'dens_moinho_2', ['VAZA1_I@08MO-BP-821I-02M1', 'VAZA1_I@08AP-BP-875I-01', 'DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-02', 'VAZA1_I@08MO-MO-821I-02', 'VAZA1_I@08MO-TQ-821I-02'])
add_model_tags(datasets, df_usina8, 'dens_moinho_3', ['VAZA1_I@08MO-BP-821I-03M1', 'VAZA1_I@08AP-BP-875I-01', 'DENS1_I@08AP-TQ-875I-03', 'VAZA1_I@08MO-BP-821I-03', 'VAZA1_I@08MO-MO-821I-03', 'VAZA1_I@08MO-TQ-821I-03'])


# MAGIC %md
# MAGIC #### Adicionando a coluna de antracito a todos os modelos com a tag PESO1_I@08MO-BW-813I-XXM1


# for q,df in datasets.items():
#     if 'PESO1_I@08MO-dBW-813I-03M1' in df.columns and 'antracito' not in df.columns:
#         df.insert(0, 'antracito', df['PESO1_I@08MO-BW-813I-03M1']+df['PESO1_I@08MO-BW-813I-04M1'])


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
# datasets['umidade'] = datasets['umidade'].drop(['max_tags_troca_l1', 'mean_DENS1', 'mean_NIVE1'], axis=1)


#analise
for k, v in datasets.items():
  if 'PESO1_I@08MO-BW-813I-01M1' in v.columns:
    log.info(k)


add_model_tags(datasets, df_usina8, 'produtividade filtragem', ['UMID_H2O_PR_L@08FI'])


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


# datasets['gas'].reset_index().plot.scatter(x = 'Processo', y='CONS1_Y@08QU-GAS', alpha=0.3, figsize=(25,6))


datasets['umidade'].columns


datasets['produtividade filtragem'].columns


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


datasets.keys()


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