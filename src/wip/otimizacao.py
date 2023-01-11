import math
import re
import os
import os.path
import sys
import glob
import warnings
import json
import pickle
import numbers
import itertools
import subprocess

import importlib
import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import shap
from datetime import datetime
from scipy.interpolate import interp1d
from IPython.display import display, clear_output

warnings.filterwarnings('ignore')


# MAGIC %md
# MAGIC # run project libraries
# MAGIC **modules**
# MAGIC * predictive_module
# MAGIC * utils
# MAGIC   * save_LP_adls
# MAGIC   * save_txt
# MAGIC * constants
# MAGIC   * constants.production_range
# MAGIC   * constants.TARGETS_IN_MODEL
# MAGIC * operations
# MAGIC   * operations.normalize_feature
# MAGIC   * operations.unnormalize_feature
# MAGIC   * operations.string_in_list
# MAGIC   * operations.scaling_target_values
# MAGIC   * operations.define_integer_scalers
# MAGIC * solver_operations
# MAGIC   * solver_operations.retrieve_model_coeficients
# MAGIC   * solver_operations.write_descriptive_contraints
# MAGIC * constraints
# MAGIC   * Constraints.write_simple_range_terms
# MAGIC * limits
# MAGIC   * Limits.define_bentonita_limit
# MAGIC   * Limits.define_limit_by_rolling_mean
# MAGIC   * Limits.define_limit_by_normalization
# MAGIC   * Limits.define_limit_by_quantile
# MAGIC * outputs
# MAGIC   * define_optimization_results
# MAGIC * pulp_solver
# MAGIC   * PulpSolver
# MAGIC 	* .solve_range
# MAGIC 	* .export_results
# MAGIC 
# MAGIC **files: Notas sobre variáveis puxadas de scripts**
# MAGIC * variáveis geradas por script de mesmo nome:
# MAGIC   * rolling_limits, norm_limits, quantile_limits, constant_limits, fixed_limits, custo_real, df_detailed
# MAGIC * diversas variáveis são puxadas do datalake via resultados do Modelos-Preditivo


us_sufix = '08'
solver_path = f'us{us_sufix}'.replace('0', '')
tmp_path = f'/dbfs/tmp/{solver_path}'


# dbutils.notebook.run('modelos-preditivo', 0)


datasets['abrasao']


import modules.predictive_module


import ....Utils.utils


#%run ../../Utils/sql_module


import .Otimizacao.modules.constants


import .Otimizacao.modules.operations


import .Otimizacao.modules.solver_operations


import .Otimizacao.modules.constraints


import .Otimizacao.modules.limits


import .Otimizacao.modules.outputs


import ....Utils.pulp_solver


import .Otimizacao.files.df_detailed


import .Otimizacao.files.rolling_limits


import .Otimizacao.files.norm_limits


import .Otimizacao.files.quantile_limits


import .Otimizacao.files.constant_limits


import .Otimizacao.files.fixed_limits


import .Otimizacao.files.custo_real


root_path = f'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/Usina{us_sufix}/Otimizacao'
path = os.path.join(root_path, 'Dependencias_Otimizacao')
log_path = os.path.join(root_path, 'logs')
azure_path = os.path.join(root_path, 'LPs')
analysis_path = os.path.join(root_path, 'analises')


# MAGIC %md
# MAGIC #carrega joblibs


models_results = load_adls(path, 'models_results.joblib', dbutils)
scalers = load_adls(path, 'scalers.joblib', dbutils)
models_coeficients = load_adls(path, 'models_coeficients.joblib', dbutils)
models_features = load_adls(path, 'models_features.joblib', dbutils)
datasets = load_adls(path, 'datasets.joblib', dbutils)

if models_results == False or datasets == False:
    raise AssertionError('Failed to retrieve file')


# MAGIC %md
# MAGIC # filter dates


for model in datasets.keys():
    datasets[model] = datasets[model][datasets[model].index < '2022-09-01']
for feature in models_features.keys():
    models_features[feature] = models_features[feature][models_features[feature].index < '2022-09-01']
    
print('last date is:', datasets['basicidade'].tail(1).index[0])


# MAGIC %md
# MAGIC #limits
# MAGIC Limits é uma classe do script modules/limits.py


rolling_limits = Limits.read_limits(rolling_limits) 
quantile_limits = Limits.read_limits(quantile_limits)
constant_limits = Limits.read_limits(constant_limits)
norm_limits = Limits.read_limits(norm_limits)


continue_limits = [
'POTE1_I@08MO-MO-821I-'
]


temp_limits = pd.DataFrame(columns = ['Range_max', 'TAG', 'Valor_Real', 'Valor_Norm', 'Ascending'])


# MAGIC %md
# MAGIC #aplica shap


shap_cols = ['compressao', 'relacao gran', 'SE PR', 'umidade', 'SE PP']
shap_cols = ['compressao', 'SE PR', 'umidade', 'SE PP']
for qualidade in shap_cols:
  for range_min, range_max in constants.production_range:
    prod_pq = False

    df_train = datasets[qualidade].copy()

    # tag para saber se usina esta produzindo ou nao
    if 'PROD_PQ_Y@08US' not in df_train.columns:
        df_train.insert(loc=0, column='PROD_PQ_Y@08US', value=datasets['gas']['PROD_PQ_Y@08US'])
        prod_pq = True

    df_train = df_train[(df_train['PROD_PQ_Y@08US'] > 0)]

    df_train = df_train.replace([np.inf, -np.inf], np.nan)
    df_train = df_train.fillna(method='bfill').fillna(method='ffill').fillna(method='bfill').fillna(0)

    status_column = [x for x in df_train.columns if 'status' in x or 'ProducaoPQ_Moagem' in x]
    if len(status_column) > 0:
        df_train.drop(status_column, axis=1, inplace=True)

    df_train_c = df_train[(df_train['PROD_PQ_Y@08US'] >= range_min) & (df_train['PROD_PQ_Y@08US'] <= range_max)].copy()
          
    if prod_pq:
        df_train_c.drop(['PROD_PQ_Y@08US'], axis=1, inplace=True)

    if df_train_c.shape[0] <= 1:
        continue

    df_result = pd.DataFrame(
              [(i['conf'], i['model'].get_params().__str__(),
        i['metrics']['mse'],
        i['metrics']['mape'],
        i['metrics']['r2'],
        i['metrics']['r'],
        i['metrics']['r2_train'],
        i['metrics']['r2_train_adj']) for i in models_results[qualidade]],     
        columns=['Modelo', 'Params', 'MSE','MAPE', 'R2', 'R', 'R2 Train', 'R2 Train Adj'])


    best_one = df_result.sort_values(by=['MAPE']).index[0]
    shap_explainer = shap.LinearExplainer(models_results[qualidade][best_one]['model'], df_train_c[df_train_c.columns[:-1]])
    train_shap_values = shap_explainer.shap_values(df_train_c[df_train_c.columns[:-1]])

    for col in df_train_c.columns:
        if 'TEMP1_I@08QU-QU-855I-GQ' not in col and qualidade in 'compressao':
            continue
        elif 'GRAN_OCS_TM@08PE-BD-840I-' not in col and qualidade in 'relacao gran':
            continue
        elif 'corpo_moedor_especifico' not in col and qualidade in 'SE PR':
            continue
        elif 'POTE1_I@08FI-BV-827I-' not in col and qualidade in 'SE PP' or col in 'POTE1_I@08FI-BV-827I-02M1':
            continue
        elif 'ROTA1_I@08FI-FL-827I-' not in col and qualidade in 'umidade':
                continue

        y = train_shap_values[:,df_train_c.columns.get_loc(col)]                
        x = df_train_c[col].values

        y_interp = interp1d(y, x)
        feat = pd.DataFrame([x,y]).T.sort_values(by=[0]).reset_index(drop=True)
        shap_norm_value = operations.normalize_feature(scalers, col, y_interp(0.0))
        temp_limits = temp_limits.append({'Range_max':range_max, 'TAG':col, 'Valor_Real':y_interp(0.0), 'Valor_Norm': shap_norm_value, 'Ascending':(feat[1][1] < feat[1][len(feat[1])-1])}, ignore_index=True)       


# MAGIC %md
# MAGIC # restricoes de tags críticas
# MAGIC Definido em e-mail de 2022-12-07, remetente Aileen, 
# MAGIC * titulo: RES: [Analytics&OCS]Limites de Processo - variáveis de SP de controles
# MAGIC obs: as tags comentadas foram mantidas sob restrição antiga por já estarem restringidas de acordo com o histórico (e haveria dificuldade de setar os valores devido à normnalização)
# MAGIC 
# MAGIC quanto a torque, o valor pedido é em percentual, mas nao corresponde com a grandeza atual, que é de 6500
# MAGIC 
# MAGIC sobre outras restrições críticas, checar em analises/EDA04_restricoes


# Limites mínimo e máximo de pressão valores entre 50 e 120 bar, desde que o limite mínimo seja menor que o limite máximo.
# Temperatura dos grupos de Queima

critical_cols_dict = {
    'dens_moinho_1': {'lmin': 2.85, 'lmax': 2.95}, 'dens_moinho_2': {'lmin': 2.85, 'lmax': 2.95}, 'dens_moinho_3': {'lmin': 2.85, 'lmax': 2.95}, 
    'SE PR': {'lmin': 1600, 'lmax': 1800}, 'GRAN PR': {'lmin': 65, 'lmax': 100}, 'cfix': {'lmin': 1, 'lmax': 1.4},
# 'GRAN_OCS_TM@08PE-BD-840I-03', 'GRAN_OCS_TM@08PE-BD-840I-06', 'GRAN_OCS_TM@08PE-BD-840I-01', 'GRAN_OCS_TM@08PE-BD-840I-02', 'GRAN_OCS_TM@08PE-BD-840I-11', 'GRAN_OCS_TM@08PE-BD-840I-05', 'GRAN_OCS_TM@08PE-BD-840I-12', 'GRAN_OCS_TM@08PE-BD-840I-08', 'GRAN_OCS_TM@08PE-BD-840I-10', 'GRAN_OCS_TM@08PE-BD-840I-07', 'GRAN_OCS_TM@08PE-BD-840I-09', 'GRAN_OCS_TM@08PE-BD-840I-04',
#     'torque': {'lmin': 60, 'lmax': 100},
    'PRES2_I@08PR-RP-822I-01': {'lmin': 50, 'lmax': 120}, 'PRES3_I@08PR-RP-822I-01': {'lmin': 50, 'lmax': 120}, 'PRES2_I@08PR-RP-822I-01': {'lmin': 50, 'lmax': 120}, 'PRES3_I@08PR-RP-822I-01': {'lmin': 50, 'lmax': 120},
    'basicidade': {'lmin': 0.95, 'lmax': 1.95},
    'TEMP1_I@08QU-QU-855I-GQ04': {800: {'lmin': 950, 'lmax': 1010}, 750: {'lmin': 840, 'lmax': 900}}, 
    'TEMP1_I@08QU-QU-855I-GQ05': {800: {'lmin': 1150, 'lmax': 1210}, 750: {'lmin': 950, 'lmax': 1010}}, 
    'TEMP1_I@08QU-QU-855I-GQ06': {800: {'lmin': 1150, 'lmax': 1210}, 750: {'lmin': 990, 'lmax': 1050}}, 
    'TEMP1_I@08QU-QU-855I-GQ07': {800: {'lmin': 1250, 'lmax': 1310}, 750: {'lmin': 1050, 'lmax': 1110}}, 
    'TEMP1_I@08QU-QU-855I-GQ08': {800: {'lmin': 1250, 'lmax': 1310}, 750: {'lmin': 1050, 'lmax': 1110}}, 
    'TEMP1_I@08QU-QU-855I-GQ09': {800: {'lmin': 1300, 'lmax': 1360}, 750: {'lmin': 1200, 'lmax': 1260}}, 
    'TEMP1_I@08QU-QU-855I-GQ10': {800: {'lmin': 1300, 'lmax': 1360}, 750: {'lmin': 1210, 'lmax': 1270}}, 
    'TEMP1_I@08QU-QU-855I-GQ11': {800: {'lmin': 1300, 'lmax': 1360}, 750: {'lmin': 1210, 'lmax': 1270}}, 
    'TEMP1_I@08QU-QU-855I-GQ12': {800: {'lmin': 1300, 'lmax': 1360}, 750: {'lmin': 1210, 'lmax': 1270}}, 
    'TEMP1_I@08QU-QU-855I-GQ13': {800: {'lmin': 1300, 'lmax': 1360}, 750: {'lmin': 1210, 'lmax': 1270}}, 
    'TEMP1_I@08QU-QU-855I-GQ14': {800: {'lmin': 1300, 'lmax': 1360}, 750: {'lmin': 1210, 'lmax': 1270}}, 
    'TEMP1_I@08QU-QU-855I-GQ15': {800: {'lmin': 1300, 'lmax': 1360}, 750: {'lmin': 1210, 'lmax': 1270}}, 
    'TEMP1_I@08QU-QU-855I-GQ16': {800: {'lmin': 1300, 'lmax': 1360}, 750: {'lmin': 1210, 'lmax': 1270}}, 
#     'TEMP1_C@08QU-PF-852I-02M1': {'lmin': 390, 'lmax': 550},
#     'PRES1_I@08QU-WB-851I-19': {'lmin': -600, 'lmax': 0},
#     'PRES2_C@08QU-PF-852I-03M1': {'lmin': 300, 'lmax': 600},
#     'TEMP1_I@08PN-TR-860I-02 ': {'lmin': 0, 'lmax': 150},
#     'PRES1_C@08QU-PF-852I-04M1': {'lmin': -300, 'lmax': -600},
#     'PRES1_I@08QU-DU-853I-12': {'lmin': 15, 'lmax': 35},
#     'TEMP1_C@08QU-PF-852I-05M1': {'lmin': 100, 'lmax': 200},
#     'PEVT_PF852I05_M1_PIT_OCS_CDV_09a12': {'lmin': -600, 'lmax': -260},
#     'PRES1_C@08QU-PF-852I-07M1': {'lmin': 120, 'lmax': 500},
#     'PRES7_I@08QU-HO-851I-01': {'lmin': 0, 'lmax': 50},
#     'PRES1_I@08QU-WB-851I-21 ': {'lmin': 350, 'lmax': 650},
#     'TEMP1_I@08QU-HO-851I-01': {'lmin': 0, 'lmax': 1150},
    
}

tags_ventiladores = ['TEMP1_C@08QU-PF-852I-02M1', 'PRES1_I@08QU-WB-851I-19', 'PRES2_C@08QU-PF-852I-03M1', 'TEMP1_I@08PN-TR-860I-02 ', 'PRES1_C@08QU-PF-852I-04M1', 'PRES1_I@08QU-DU-853I-12', 'TEMP1_C@08QU-PF-852I-05M1', 'PEVT_PF852I05_M1_PIT_OCS_CDV_09a12', 'PRES1_C@08QU-PF-852I-07M1', 'PRES7_I@08QU-HO-851I-01', 'PRES1_I@08QU-WB-851I-21 ', 'TEMP1_I@08QU-HO-851I-01']

# adjust min and max to historical values on tags_ventiladores
missing_tags = []
for tag, limits in critical_cols_dict.items():
#     if tag in tags_ventiladores or tag.startswith('TEMP1_I@08QU-QU'):
    if tag in tags_ventiladores:
            
        if tag in tags_ventiladores:
            limits_min = limits['lmin']
            limits_max = limits['lmax']

        historical_min = scalers[tag].data_min_[0]
        historical_max = scalers[tag].data_max_[0]
        if historical_min > limits_min: 
            critical_cols_dict[tag]['lmin'] = historical_min
        if historical_max < limits_max: 
            critical_cols_dict[tag]['lmax'] = historical_max
            
#     elif tag.startswith('TEMP1_I@08QU-QU'):
#         production = datasets['finos']['PROD_PQ_Y@08US']
#         for temp_range in [750, 800]:
#             if temp_range > 750: 
#                 production_query = (production > temp_range).copy()
#             else: 
#                 production_query = (production <= temp_range).copy()
#             limits_min = limits[temp_range]['lmin']
#             limits_max = limits[temp_range]['lmax']
            
#             production_query = production_query.filter(models_features[tag].index).loc[production_query == True].index
#             feature_in_prod = models_features[tag][production_query]
#             historical_min = operations.unnormalize_feature(scalers, tag, feature_in_prod.min(), 'one_operation')
#             historical_max = operations.unnormalize_feature(scalers, tag, feature_in_prod.max(), 'one_operation')
            
#             if historical_min > limits_min: 
#                 critical_cols_dict[tag][temp_range]['lmin'] = historical_min
#             if historical_max < limits_max: 
#                 critical_cols_dict[tag][temp_range]['lmax'] = historical_max
    elif tag not in scalers: 
            missing_tags.append(tag)
            continue
            
# remove missing
for tag in missing_tags:
    critical_cols_dict.pop(tag)
print(missing_tags)
        
# captura nomes reais de tags
critical_cols_dict = {(list(constants.TARGETS_IN_MODEL.keys())[list(constants.TARGETS_IN_MODEL.values()).index(c)])
                if c in constants.TARGETS_IN_MODEL.values() else c:val
                for c, val in critical_cols_dict.items()}


# MAGIC %md
# MAGIC # escreve constraints


def build_restrictions(tmp_path, models_coeficients, test_restrictions = False):
    production = datasets['finos']['PROD_PQ_Y@08US']
    production_pc = datasets['relacao gran']['PROD_PC_I@08US']

    for range_min, range_max in constants.production_range:
#     for range_min, range_max in [(800, 850)]:
#     for range_min, range_max in [(800, 850), (850, 900), (900, 950)]:
#         log.info(f'{range_min},{range_max}')

        with open(os.path.join(tmp_path, f'restricoes-faixa-{range_min}-{range_max}.txt'), 'w') as constraint_files:
            for model in models_results:
                features_coeficient = solver_operations.retrieve_model_coeficients(model, models_results)
                descriptive_args = {'file': constraint_files, 'model_target': model,
                                    'datasets': datasets, 'df_detailed': df_detailed,
                                    'scalers': scalers,
                                    'models_coeficients': models_coeficients,
                                    'features_coeficient': features_coeficient,
                                    'models_results': models_results}

                models_coeficients = solver_operations.write_descriptive_contraints(**descriptive_args)
            print('end of model constraints')

            production_query = ((production >= range_min) & (production <= range_max) & (production_pc >= range_min)).copy()
            log.info(f'data size {sum(production_query)}')

            features_limits = {}
            for feature in models_features.keys():
                new_feature = str()
                if feature in constants.TARGETS_IN_MODEL.keys():
                    new_feature = constants.TARGETS_IN_MODEL[feature]

                cond_one = operations.string_in_list(new_feature, continue_limits)
                cond_two = operations.string_in_list(feature, continue_limits)

                if cond_one or cond_two or feature.startswith('FUNC') or feature in constants.TARGETS_IN_MODEL.values() or feature in datasets.keys():
                    continue  

                production_query = ((production >= range_min) & (production <= range_max)).copy()            
                models_features[feature] = models_features[feature].groupby(level=0).first()
                production_query = production_query.filter(models_features[feature].index)
                production_query = production_query.loc[production_query == True].index
                feature_in_prod = models_features[feature][production_query]

                if 'CALC1_Y@08FI-FD00' in feature:
                    lmin = feature_in_prod.quantile(0.25)
                    lmax = feature_in_prod.quantile(0.9)

                elif feature in ['PESO1_I@08MO-BW-813I-03M1', 'PESO1_I@08MO-BW-813I-04M1']:
                    lmin = feature_in_prod.quantile(0.8)
                    lmax = feature_in_prod.quantile(0.95)

                elif feature in ['TEMP1_I@08QU-QU-855I-GQA', 'TEMP1_I@08QU-QU-855I-GQB', 'TEMP1_I@08QU-QU-855I-GQC', 'TEMP1_I@08QU-QU-855I-GQ01', 'TEMP1_I@08QU-QU-855I-GQ02']:
                    lmin = feature_in_prod.quantile(0.4)
                    lmax = feature_in_prod.quantile(0.9)

                elif feature in set(temp_limits.TAG.values) and 'POTE1_I@08FI-BV-827I-' not in feature and 'ROTA1_I@08FI-FL-827I-' not in feature:
                    lmin = feature_in_prod.mean() - (feature_in_prod.std() )
                    lmax = feature_in_prod.mean() + (feature_in_prod.std() )

                    if range_max in temp_limits['Range_max'].values:
                        value = temp_limits[(temp_limits['Range_max'] == range_max) & (temp_limits['TAG'] == feature)]
                        if value.shape[0] == 0:
                            continue
                        ascending = value['Ascending'].values[0]
                        lmin = value['Valor_Norm'].values[0]
                        lmax = value['Valor_Norm'].values[0]

                        if feature not in ['TEMP1_I@08QU-QU-855I-GQ01', 'TEMP1_I@08QU-QU-855I-GQ03', 'TEMP1_I@08QU-QU-855I-GQ04', 'TEMP1_I@08QU-QU-855I-GQ05', 'TEMP1_I@08QU-QU-855I-GQ15', 'TEMP1_I@08QU-QU-855I-GQ16'] \
                                    and 'ROTA1_I@08FI-FL-827I-' not in feature:

                          if ascending:
                              lmin = value['Valor_Norm'].values[0]
                              lmax = value['Valor_Norm'].values[0] + feature_in_prod.std()
                          else:
                              lmin = value['Valor_Norm'].values[0] - feature_in_prod.std()
                              lmax = value['Valor_Norm'].values[0]

                elif 'ROTA1_I@08QU-PF-852I-' in feature:
                    lmin = feature_in_prod.quantile(0.3)
                    lmax = feature_in_prod.quantile(0.75)
                    
                elif feature.startswith('GRAN_OCS') and not feature.startswith('GRAN_OCS_TM') and feature != 'GRAN_-0,045_PR_L@08FI':
                    lmin = feature_in_prod.quantile(0.25)
                    lmax = feature_in_prod.quantile(0.75)

                elif feature in fixed_limits:
                    lmin = lmax = models_features[feature][production_query].mean()

                elif 'Consumo de Energia (base minério úmido) kWh/ton' in feature:
                  lmin = 0
                  lmax = models_features[feature][production_query].max()

                elif operations.string_in_list(feature, constant_limits.keys()):
                    lmin, lmax = Limits.define_constant_limits(feature, constant_limits)

                elif feature == "qtde_filtros":
                  lmin = 5
                  lmax = 10  

                elif feature == "bentonita":
                    production_query = (production >= range_min) & (production <= range_max)
                    lmin, lmax = Limits.define_bentonita_limit(feature, datasets, production_query, scalers)

                elif feature in rolling_limits.keys():
                    production_query = (production >= range_min) & (production <= range_max)
                    lmin, lmax = Limits.define_limit_by_rolling_mean(feature, production_query,
                                                                     datasets, scalers, rolling_limits)

                elif feature in norm_limits.keys():
                    production_query = (production >= range_min) & (production <= range_max)
                    lmin, lmax = Limits.define_limit_by_normalization(scalers, feature, norm_limits)

                    if feature in ["NIVE7_I@08QU-FR-851I-01M1", "ROTA1_I@08QU-PF-852I-06M1"]:
                        lmax = models_features[feature][production_query].max()

                elif feature in quantile_limits.keys():
                    lmin, lmax = Limits.define_limit_by_quantile(feature,
                                                                 models_features, production_query, quantile_limits)
                    if feature == "rotacaoPeneiraAvg":
                        lmax = models_features[feature][production_query].max()

                # else: pega min e max historico
                else:
                    lmin = models_features[feature][production_query].min()
                    lmax = models_features[feature][production_query].max()

                if (lmin is None) or (lmax is None) or (pd.isna(lmin)) or (pd.isna(lmax)):
                    lmin =  models_features[feature][production_query].min() if lmin is None else lmin
                    lmax = models_features[feature][production_query].max() if lmax is None else lmax

                lmin, lmax = float(lmin), float(lmax)
                features_limits[feature] = {"lmin": lmin, "lmax": lmax}            
                cond_one = feature in constants.TARGETS_IN_MODEL.keys()

                # se target, desnormaliza
                # se deixar vazio em vez de 'one_operation', vai operar com 2 valores: o desnormalizado e o valor dividido pelo range
                if cond_one and ( constants.TARGETS_IN_MODEL[feature] in models_features.keys()):
                    lmin = operations.unnormalize_feature(scalers, feature, lmin, 'one_operation')
                    lmax = operations.unnormalize_feature(scalers, feature, lmax, 'one_operation')

#                 definicoes de restricao via email aileen, ver sessao correspondente acima. ATENCAO: sobrescreve outras condições
                if feature in critical_cols_dict:
                    if feature.startswith('TEMP1_I@08QU-QU'):
                        if range_max > 750:
                            corte = 800
                        else: corte = 750
                        lmin = operations.normalize_feature(scalers, feature, critical_cols_dict[feature][corte]['lmin'])
                        lmax = operations.normalize_feature(scalers, feature, critical_cols_dict[feature][corte]['lmax'])
                    elif feature.startswith('PRES') or feature in tags_ventiladores:
                        lmin = operations.normalize_feature(scalers, feature, critical_cols_dict[feature]['lmin'])
                        lmax = operations.normalize_feature(scalers, feature, critical_cols_dict[feature]['lmax'])
                    else:
                        lmin = critical_cols_dict[feature]['lmin']
                        lmax = critical_cols_dict[feature]['lmax']
                        feature = constants.TARGETS_IN_MODEL[feature]
                    Constraints.write_feature_constraints(feature, constraint_files, lmin, lmax)
                    continue
                    
                if feature not in constants.TARGETS_IN_MODEL.keys() or 'Calculo' in feature: # or 'ROTA1_I@08PE-BD-840I-' in feature :
                    Constraints.write_feature_constraints(feature, constraint_files, lmin, lmax)

                if feature in constants.TARGETS_IN_MODEL.keys() or 'rota_disco_' in feature:
                    new_lmin, new_lmax, new_feature = operations.scaling_target_values(feature, scalers,
                                                                                       lmin, lmax)

                    if new_feature in ['energia_moinho']:
                      new_lmin = 0

                    Constraints.write_feature_constraints(new_feature, constraint_files, new_lmin, new_lmax)

            #termina gravacao dos modelos, inicia definicao manual de restricoes
            #
            #write_simple_range_terms opera com o file range_constraints
            Constraints.write_simple_range_terms(constraint_files, scalers, features_limits)

            Constraints.write_simple_constraints(constraint_files)

            Constraints.parse_range_complex_constraints(constraint_files, scalers)

            # special constraints are related to specific adjustments, generaly defined at seven_plant script
            production_query = (production >= range_min) & (production <= range_max)
            Constraints.write_special_constraints(constraint_files, scalers, datasets, production_query, range_min, range_max)

            Constraints.write_complex_constraints(constraint_files, scalers)

            Constraints.write_variable_constraints(constraint_files, features_limits, scalers, range_min, range_max)

            Constraints.write_targets_limits(constraint_files, datasets, features_limits)

        operations.replace_string_from_file(tmp_path, range_min, range_max)


if not(os.path.exists(tmp_path)):
    os.mkdir(tmp_path)
    
# salva costs.csv
with open(f'{tmp_path}/custo_real.json', 'w') as fp:
    json.dump(custo_real, fp)
write_objective_function_coef(tmp_path, scalers)

# constroi restricoes
test_restrictions = False
build_restrictions(tmp_path, models_coeficients, test_restrictions = test_restrictions)
if test_restrictions == False:
    diffchecker_sufix = '_standard'
if test_restrictions == True:
    diffchecker_sufix = '_filtered'


# MAGIC %md
# MAGIC # aplica cbc


# solver = PulpSolver(tmp_path, os.path.join(tmp_path, 'custo_real.json'), constants, us_sufix[1], 'cbc')
solver = PulpSolver(tmp_path, os.path.join(tmp_path, 'custo_real.json'), 'cbc')


solver.solve_range(ranges=None, tmp_path=tmp_path, us=us_sufix.replace('0', ''))
solver.export_results()


# MAGIC %md
# MAGIC # salva lp


for range_min, range_max in constants.production_range:
    save_LP_adls(azure_path, f'{range_min}-{range_max}', solver, dbutils)

# solver.probs['800-850'].writeLP('/dbfs/tmp/us8/lpfiles/800-850.lp')


# MAGIC %md
# MAGIC # gera output


df_all = pd.concat(datasets.values(), axis=1)
df_all = df_all.loc[:, ~df_all.columns.duplicated()]

scalers = operations.define_integer_scalers(df_all, scalers)

df_output = define_optimization_results(tmp_path, scalers, datasets)


# MAGIC %md
# MAGIC #salva resultados do otimizador


time = datetime.today().strftime('%Y-%m-%d')
log.info(path)
save_csv(path, f'resultado_otimizador-US{us_sufix}_{time}.csv', df_output, dbutils)


# MAGIC %md
# MAGIC # salva arquivos no datalake para analise


for i in os.listdir(tmp_path):
    if i.startswith('restricoes-faixa'):
        log.info(i)
        with open(tmp_path+'/'+i, 'r') as f:
#         with open('us8/'+i, 'r', encoding='ISO-8859-1') as f: # testar com esse encoding
            text = f.read()
            if len(text) == 0: 
                raise AssertionError ("text is empty")
            save_txt(analysis_path, i, text, dbutils)
    if i == 'custo_real.json':
        with open(tmp_path+'/'+i, 'r') as f:
            text = f.read()
            save_txt(analysis_path, i, text, dbutils)


# MAGIC %md
# MAGIC # testes/rascunhos


