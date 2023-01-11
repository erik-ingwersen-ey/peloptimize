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

import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt

from sklearn.model_selection import KFold, train_test_split
from IPython.display import display
# from ipywidgets import widgets
from IPython.display import display, clear_output
from sklearn.linear_model import Ridge, Lasso
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import mean_squared_error, r2_score
import pytz
import joblib
import warnings
warnings.filterwarnings('ignore')

# import dotenv
# dotenv.load_dotenv()

from datetime import datetime, timedelta


# MAGIC %md
# MAGIC **Notas sobre variáveis puxadas de scripts**
# MAGIC * preprocessamento gera variáveis: df_sql e datasets
# MAGIC * df_detailed gera variável df_detailed, que não é usada...


# MAGIC %md
# MAGIC # run project libraries


print(datetime.now())

data_path, data_file, df_sql_file = eval(dbutils.notebook.run("preprocessamento", 0))

datasets = joblib.load(os.path.join(data_path, data_file))
# df_sql = joblib.load(os.path.join(data_path, df_sql_file))


import ....Utils.utils


import .Otimizacao.modules.operations


import .Otimizacao.modules.predictive_module


import .Otimizacao.files.df_detailed


import ....Utils.model_module


#%run ../Otimizacao/modules/model_module


import .Otimizacao.modules.predictive_module


# funcao nao utilizada. Remove quaisquer espaços no inicio e fim dos nomes das colunas
# def rename_data(df): # Callback
#     # Transformando a coluna 'Processo' para datetime
#     df['Processo'] = pd.to_datetime(df['Processo'], dayfirst=True)
    
#     # Definindo o indice do dataframe
#     df = df.set_index('Processo')
    
#     # Removendo quaisquer espaços no inicio e fim dos nomes das colunas
#     df.columns = [c.strip() if not pd.isnull(c) else '' for c in df.columns]
    
#     return df

# ### Making maps


# df = df_sql.copy()


# MAGIC %md
# MAGIC ## Models


models = {
    'ridge': {
        'model': Ridge,
        'params': {
            'alpha': [100,10,1,0.1,0.01,0.001],
        }
    },
}

results = {}
scalers = {}
limits = {}
nive_concat = pd.DataFrame()
cv_n = 3
cv_size = 0.3


scalers, all_df = operations.define_real_scalers(datasets)


datasets['particulados2'].drop(['TEMP1_I@08QU-PP-871I-03-PAT02', 'VOLT1_I@08QU-PP-871I-03-PAT01'], axis=1, inplace=True)


# datasets['SUP_SE_PP'] = datasets['SE PP'].copy()
# datasets['SUP_SE_PP']['SUP_SE_PP_L@08PR'] = datasets['taxarp']['SUP_SE_PP_L@08PR'].copy()
# datasets['SUP_SE_PP'] = datasets['SUP_SE_PP'].drop(['GANHO PRENSA - US8'], axis=1)


for qualidade in datasets.keys():
# for qualidade in ['dens_moinho_1', 'dens_moinho_2', 'dens_moinho_3']:

    if qualidade.startswith('cm'): continue
    print('-----', qualidade)

    results[qualidade] = []
    df_train = datasets[qualidade].copy()
#     df_train = df_train.replace([np.inf, -np.inf], np.nan)
#     df_train = df_train.fillna(method='bfill').fillna(method='ffill').fillna(method='bfill').fillna(0)
    col_target = df_train.columns[-1]
    print('DEBUG - target: ', col_target)

    # ----------------------------------------------------------------------------
    
    for c in ['GANHO PRENSA - US8']: 
        if c in df_train.columns[:-1]:
            df_train[c] = np.log(df_train[c])

    # --------------------------------------------------------------------
    
    # Aplicando filtros sobre o df_train
    print(df_train.shape)
    df_train = mod_filtros(df_train, qualidade, col_target)
    print(df_train.shape)
    # Passando por todas as colunas do df (exceto a target)
    for t in df_train.columns[:-1]:
        
        # Definindo as colunas que serão ignoradas
        if t in ['status','ProducaoPQ_Moagem'] or t.startswith('qtde') or t.startswith('SOMA FUNC'):
            df_train[t].fillna(df_train[t].mode()[0], inplace=True)
            continue
        
        # Aplicando tratamento nos dados (fillna e rolling)
        df_train = tratando_dados(df_train, qualidade, t, restrict_to=[
            'abrasao','basicidade','distribuicao gran','finos','GRAN PR','SE PR', 'dens_moinho_1', 'dens_moinho_2', 'dens_moinho_3'
        ])
        
        # Gerando o scaler dos NIVE
        if t in [
            'NIVE1_C@08QU-FR-851I-01M1', 'NIVE2_I@08QU-FR-851I-01M1',
            'NIVE3_I@08QU-FR-851I-01M1', 'NIVE4_I@08QU-FR-851I-01M1',
            'NIVE5_I@08QU-FR-851I-01M1', 'NIVE6_I@08QU-FR-851I-01M1'
        ] and t not in scalers:
            nive_concat = pd.concat([nive_concat, df_train[t]], ignore_index=True)
        
        # Criando os scalers dos atributos
        if t not in scalers:
            print('tag entrou no if. Dai faz um fit nela pq deve ter sido ignorado na etapa de geracao dos scalers. Parece uma gambiarra do Leandro', t)
            scalers[t] = MinMaxScaler()

            tmp = df_train[[t]].copy()
            if t.startswith('DENS1_C@08HO-BP'):
                raise AssertionError ('verificando se esse if é acionado. Parece gambiarra do Leandro')
                tmp[t][0] = 0

            scalers[t].fit(tmp.astype(float))

        # Escalonando os valores

        df_train[t] = scalers[t].transform(df_train[[t]]) # os valores de filtragem sao todos negativos,aqui ele ta cortando eles ai o dataframe fica zerado

    # Selecionando apenas os elementos válidos
    print('pre remoção status zerado:', df_train.shape)
    if 'status' in df_train.columns:
        df_train = df_train.query('status == 1').drop('status', axis=1)
    print('pós remoção status zerado',df_train.shape)
    
    
    #Aplicando um filtro de elementos 
    if 'ProducaoPQ_Moagem' in df_train:
        p1 = df_train['ProducaoPQ_Moagem']>=700
        p2 = df_train['ProducaoPQ_Moagem']<=1000
        df_train = df_train[p1&p2].drop('ProducaoPQ_Moagem', axis=1)

        
    # Aplicando filtro de target maior que 0
    if qualidade.startswith('energia') or qualidade in ['gas']:
        df_train = df_train[df_train[col_target]>0]
        print(df_train.shape)

#     df_train.drop(df_train.columns[df_train.columns.str.startswith('qtde')], axis=1, inplace=True)

    df_train = df_train.replace([np.inf, -np.inf], np.nan)
    df_train = df_train.fillna(method='bfill').fillna(method='ffill').fillna(method='bfill').fillna(0)
    
    # Gerando o treino e target
    df_train = df_train.dropna(subset=[col_target])           
    df_target = df_train[col_target]
    df_train = df_train.drop(col_target, axis=1)

    
    # Dados para os limites de cada atributo
    for c in df_train.columns:
        limits[c] = df_train[c] 
    
    # Avaliando elementos e erificando se elas estão em determinado threshold
#     if qualidade in ['particulados2','particulados3']:
#         for i,v in enumerate(['2016-11-01 12:00:00','2017-08-15 12:00:00','2017-03-20 12:00:00','2016-04-01 12:00:00']):
#             var_thres = 'thres_'+str(i)+'_'+qualidade
#             df_train[var_thres] = 0
#             df_train.loc[df_train.index > v, var_thres] = 1           
        
    # Gerando o threshold do cross fold validation
    
    print(df_train.shape)
    ix = df_train.index.sort_values()
    cv_thresholds = [ix[int(len(ix) * (1-i*cv_size/cv_n))-1] for i in range(cv_n+1)][::-1]
    
    
    # Executando os modelos
    for method, model_conf in models.items():
        print(method)
#         print('DEBUG: ', df_target)
        results[qualidade] += apply_model(model_conf, qualidade, method, df_train, df_target,
                                validation=manual_kfold_validation,
                                plot_charts=plot_charts,                 
                                selfTrain=False,
                                param_validation= {
                                    'cv_thresholds' : cv_thresholds,
                                    'qualidade' : qualidade,
                                }
                            )
        
print('Fim')


# MAGIC %md
# MAGIC # Results


models_features = limits
models_results = results


models_coeficients = {}

for model in results.keys():

    best = retrieve_best_model(results, model, 'mape')
    show_best_result(model, best)

    coefficients = { col : coef for col, coef in zip(best['columns'], best['model'].coef_) }
    models_coeficients[model] = coefficients


nive_scaler = MinMaxScaler()
nive_scaler.fit(nive_concat)
scalers['nive'] = nive_scaler


path = 'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/Usina08/Otimizacao/Dependencias_Otimizacao/'
save_model_adls(path, 'models_results.joblib', models_results)
save_model_adls(path, 'scalers.joblib', scalers)
save_model_adls(path, 'models_coeficients.joblib', models_coeficients)
save_model_adls(path, 'models_features.joblib', models_features)
save_model_adls(path, 'datasets.joblib', datasets)


# MAGIC %md
# MAGIC # testes


