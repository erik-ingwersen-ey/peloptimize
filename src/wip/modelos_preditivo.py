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
from datetime import datetime
from scipy.interpolate import interp1d
from sklearn.model_selection import KFold, train_test_split
from sklearn.linear_model import Ridge, Lasso
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import mean_squared_error, r2_score
from IPython.display import display, clear_output

import pathlib

warnings.filterwarnings('ignore')


# MAGIC %md
# MAGIC **Notas sobre variáveis puxadas de scripts**
# MAGIC * preprocessamento gera variáveis: datasets
# MAGIC * df_detailed gera variável df_detailed, que não é usada...


# MAGIC %md
# MAGIC # run project libraries
# MAGIC Mapeamento de pastas, arquivos e funções:
# MAGIC 
# MAGIC **modules**
# MAGIC * predictive_module
# MAGIC * model_module
# MAGIC   * apply_model
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


# print(datetime.now())

# data_path, data_file, df_sql_file = eval(dbutils.notebook.run("preprocessamento", 0))

# datasets = joblib.load(os.path.join(data_path, data_file))


data_path = '/dbfs/tmp/us8'
data_file = 'datasets.joblib'
print(datetime.now())

datasets = joblib.load(os.path.join(data_path, data_file))


us_sufix = '08'
solver_path = f'us{us_sufix}'.replace('0', '')


import ....Utils.utils


import .Otimizacao.modules.operations


import .Otimizacao.modules.predictive_module


import .Otimizacao.files.df_detailed


import ....Utils.model_module


#%run ../Otimizacao/modules/model_module


import .Otimizacao.modules.predictive_module


# MAGIC %md
# MAGIC # set functions


def transfer_file(from_path, to_path, filename):     
    try:
        dbutils.fs.cp(os.path.join(from_path, filename), os.path.join(to_path, filename))
    except Exception as e:
        print(e)
        return False


def apply_naive_model(X, y):
#     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=1/10, shuffle=False, random_state=42)
    X_test = X[X.index > test_set_date]
    X_train = X[X.index < test_set_date]
    y_test = y[y.index > test_set_date]
    y_train = y[y.index < test_set_date]    
    
    predicted = [y_train.mean()]*len(y_test)
    
    print(len(y_test), len(predicted))
    mse = mean_squared_error(y_test, predicted)
    mape = np.mean(np.abs(y_test - predicted) / np.abs(y_test)) * 100
    r2 = r2_score(y_test, predicted)
    mse, mape, r2 = round(mse, 3), round(mape, 3), round(r2, 3)
    
    return mse, mape, r2


# MAGIC %md
# MAGIC # checkup


print(datasets['abrasao'].index.min())
print(datasets['abrasao'].index.max())


datasets['abrasao'].describe()


# MAGIC %md
# MAGIC # filter date


# testset = datasets.copy()
# for model, df in testset.items():
#     testset[model] = df[df.index < '2022-09-13']
#     testset[model] = df[df.index >= '2022-07-13']


for model, df in datasets.items():
    datasets[model] = df[df.index < '2022-08-13']


# MAGIC %md
# MAGIC # filter outlier
# MAGIC based on specific values (not on statistics)
# MAGIC * decisão tomada por Rodrigo em 1/12/2022. Baseado em necessidade de avaliar comportamento em cima de valores mais comuns às usinas.


model = 'abrasao'
df = datasets[model]
datasets[model] = df[(df[df.columns[-1]] >= 4.5) 
                                          & (df[df.columns[-1]] < 6)]

model = 'compressao'
df = datasets[model]
datasets[model] = df[(df[df.columns[-1]] >= 230) 
                                          & (df[df.columns[-1]] < 330)]

# #usa datas especificas, ous seja, nao filtra no teste
# model = 'abrasao'
# df = datasets[model]
# datasets[model] = df[((df[df.columns[-1]] >= 4.5) 
#                                           & (df[df.columns[-1]] < 6)) | (df.index < '2022-07-01')]

# model = 'compressao'
# df = datasets[model]
# datasets[model] = df[((df[df.columns[-1]] >= 230) 
#                                           & (df[df.columns[-1]] < 330)) | (df.index < '2022-07-01')]


# MAGIC %md
# MAGIC # Models


models = {
    'ridge': {
        'model': Ridge,
        'params': {
            'alpha': [100,10,1,0.1,0.01,0.001],
        }
    },
}

cv_n = 3
cv_size = 0.3


# col_selection = ['PESO1_I@08PN-TR-860I-09M1', 'POSI1_I@08QU-VD-853I-46M1', 'PRES1_C@08QU-PF-852I-06M1', 'PRES1_I@08QU-WB-851I-01', 'antracito', 'TEMP1_I@08QU-QU-855I-GQ04', 'TEMP1_I@08QU-HO-851I-01', 'POSI1_D@08QU-VD-853I-05M1', '%CF. ANT']


test_set_date = '2022-06-01'


bad_models = ['finos', 'SE PP', 'particulados1', 'particulados2', 'particulados3', 'energia_prensa', 'custo_SE PP', 'produtividade filtragem', 'taxarp']
low_error_models = [c for c in datasets.keys() if c not in bad_models]
main_models = ['abrasao', 'basicidade', 'distribuicao gran', 'GRAN PR', 'SE PR', 'energia_forno', 'umidade']

# model_group = ['abrasao', 'compressao', 'energia_forno', 'gas']
model_group = datasets.keys()


set_type = 'test'
apply_baseline = False


def get_prediction(set_type, apply_baseline = False):
    """
    set_type: validation or test
    """
    results = {}
    scalers = {}
    limits = {}
    nive_concat = pd.DataFrame()

    scalers, all_df = operations.define_real_scalers(datasets)

    df_target_dict = {}
    for qualidade in model_group:
    # for qualidade in ['abrasao']:
      if qualidade.startswith('cm'): continue
      print('-----', qualidade)

      results[qualidade] = []
      df_train = datasets[qualidade].copy()

    #       # remove periodos com variavel constante por até 5h seguidas, exemplo: energia_moinho
    #       check_constant = df_train.iloc[:, -1]
    #       df_train[(check_constant != check_constant.shift(periods=-1)) & ((check_constant != check_constant.shift(periods=-5)) | (check_constant != check_constant.shift(periods=5)))]
    #       df_train.drop(df_train.tail(1).index, inplace=True) # drop last n rows

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
    #           if t not in scalers and set_type == 'validation':
          if t not in scalers:
              print('tag entrou no if. Dai faz um fit nela pq deve ter sido ignorado na etapa de geracao dos scalers. Parece uma gambiarra do Leandro', t)
              scalers[t] = MinMaxScaler()

              tmp = df_train[[t]].copy()
              if t.startswith('DENS1_C@08HO-BP'):
    #                   raise AssertionError ('verificando se esse if é acionado. Parece gambiarra do Leandro')
                  tmp[t][0] = 0

    #           else:
    #           print('scalers are already defined')
              scalers[t].fit(tmp.astype(float))

          # Escalonando os valores

          # ATENCAO: DATA LEAKAGE OCORRE NESSE MOMENTO
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
      y_train = df_train[col_target]
      df_train = df_train.drop(col_target, axis=1)
    #       df_train = df_train[col_selection]

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
      if set_type == 'validation':
          cv_thresholds = [ix[int(len(ix) * (1-i*cv_size/cv_n))-1] for i in range(cv_n+1)][::-1]
      elif set_type == 'test':
          cv_thresholds = [test_set_date, ix[-1]]

      # Executando os modelos
      for method, model_conf in models.items():
          print(method)
          if apply_baseline == False:
              results[qualidade] = apply_model(model_conf, qualidade, method, df_train, y_train,
                                      solver_path,
                                      validation=manual_kfold_validation,
                                      plot_charts=plot_charts,                 
                                      selfTrain=False,
                                      param_validation= {
                                          'cv_thresholds' : cv_thresholds,
                                          'qualidade' : qualidade,
                                      }
                                  )
          elif apply_baseline == True:
              results[qualidade] = apply_naive_model(df_train, y_train)
    #         main_cols_dict[qualidade] = get_main_cols()
    df_target_dict[qualidade] = y_train
    return results, scalers, limits, nive_concat, df_target_dict


# MAGIC %md
# MAGIC ## apply baseline


baseline_results, _, _, _, _ = get_prediction(set_type='test', apply_baseline = True)


# MAGIC %md
# MAGIC ## apply model


models_results, scalers, limits, nive_concat, df_target_dict = get_prediction(set_type='test', apply_baseline = False)


for model, df in datasets.items():
    for c in df.columns:
        if c.startswith('GRAN'):
            print(model, c)

# for c in df_train.columns:
#     print(qualidade, c)
#     limits[c] = df_train[c] 


# MAGIC %md
# MAGIC # Results


models_features = limits
# models_results = results


temp_dict = {}
models_coeficients = {}

for model in models_results.keys():

    best = retrieve_best_model(models_results, model, 'mape')
    temp_dict[model] = {'mape': round(best['metrics']['mape'], 3), 'r2': round(best['metrics']['r2'], 3)}
    show_best_result(model, best)

    coefficients = { col : coef for col, coef in zip(best['columns'], best['model'].coef_) }
    models_coeficients[model] = coefficients


nive_scaler = MinMaxScaler()
nive_scaler.fit(nive_concat)
scalers['nive'] = nive_scaler


# MAGIC %md
# MAGIC ## compara com baseline


models_df = (pd.DataFrame(temp_dict)
         .append(pd.DataFrame(baseline_results), ignore_index=True)
    .T
    .rename({0: 'mape', 1: 'r2', 3: 'baseline_mape', 4: 'baseline_r2'}, axis=1)
    .drop([2], axis=1)
    )
models_df['delta_mape %'] = ((models_df['baseline_mape'] - models_df['mape']) / models_df['baseline_mape']).apply(lambda x: round(x*100, 2))
models_df = models_df.reset_index().rename({'index': 'model'}, axis=1)
models_df.head()


# MAGIC %md
# MAGIC # compara com modelos de queima
# MAGIC * '+ modificacao pre filtragem de outlier. OBS: outlier nao deveria ter sido filtrado no teste.


models_df.loc[models_df['model'] == 'abrasao', ['baseline_mape']] = 9.552
models_df.loc[models_df['model'] == 'compressao', ['baseline_mape']] = 5.773
models_df['delta_mape %'] = ((models_df['baseline_mape'] - models_df['mape']) / models_df['baseline_mape']).apply(lambda x: round(x*100, 2))


modelos_queima = ['abrasao', 'compressao', 'gas', 'energia_forno', 'relacao_gran', 'cfix', 'temp_forno', 'temp_recirc', 'finos']
#finos saiu?
models_df[models_df['model'].isin(modelos_queima)]


# MAGIC %md
# MAGIC # save results


path = 'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/Usina08/Otimizacao/Dependencias_Otimizacao/'
save_model_adls(path, 'models_results.joblib', models_results, dbutils)
save_model_adls(path, 'scalers.joblib', scalers, dbutils)
save_model_adls(path, 'models_coeficients.joblib', models_coeficients, dbutils)
save_model_adls(path, 'models_features.joblib', models_features, dbutils)
save_model_adls(path, 'datasets.joblib', datasets, dbutils)


# MAGIC %md
# MAGIC ## scores


us_sufix = '08'

tmp_path = f'/dbfs/tmp/us{us_sufix.replace("0", "")}/Graficos'
pathlib.Path(tmp_path).mkdir(parents=True, exist_ok=True)
from_path = tmp_path.replace('dbfs/', '')
to_path = f'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/analises/EDA/Usina08'

version = datetime.now().strftime('%Y-%m-%d-%Hh%Mm')
filename = f'scores_{version}.csv'
models_df.to_csv(os.path.join(tmp_path, filename))
transfer_file(from_path, to_path, filename)


# MAGIC %md
# MAGIC ## save model csvs for checkup 
# MAGIC * clientes costumam demandar tais dados ocasionalmente


for plant_model in datasets.keys():
    # plant_model = 'abrasao'
    path = f'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/analises/EDA/Usina{us_sufix}/models_csv'
    save_csv(path, f'{plant_model}.csv', datasets[plant_model], dbutils)


# MAGIC %md
# MAGIC ##powerbi analytics


# path = f'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/powerbi/Usina{us_sufix}/'

# scores = pd.DataFrame(columns = ['model', 'algoritmo_ml', 'mape', 'r2'])
# for model in models_results.keys():
#     best = retrieve_best_model(models_results, model, 'mape')
#     scores = scores.append({'model': model, 'algoritmo_ml': best['conf'], 'mape': round(best['metrics']['mape'],2), 'r2': round(best['metrics']['r2'],2)}, ignore_index=True)

# scores.drop(['algoritmo_ml'], axis=1, inplace=True)
# save_csv(path, 'scores.csv', scores, dbutils, sep=',')


best = retrieve_best_model(models_results, 'abrasao', 'mape')
best['model']


predict = best['model'].predict(datasets['abrasao'][datasets['abrasao']['status']==1].drop(columns = ['ABRA_-0,5_PQ_L@08QU', 'status']).fillna(0))
predict


# MAGIC %md
# MAGIC # testes


