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


# MAGIC %md
# MAGIC **Notas sobre variáveis puxadas de scripts**
# MAGIC * variáveis geradas por script de mesmo nome:
# MAGIC   * rolling_limits, norm_limits, quantile_limits, constant_limits, fixed_limits, custo_real, df_detailed
# MAGIC * diversas variáveis são puxadas do datalake via resultados do Modelos-Preditivo


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


import utils
import us8.constants
import us8.operations
import us8.solver_operations
import us8.constraints
import us8.limits
import us8.outputs
import pulp_solver

importlib.reload(utils)
importlib.reload(us8.constants)
importlib.reload(us8.operations)
importlib.reload(us8.solver_operations)
importlib.reload(us8.constraints)
importlib.reload(us8.limits)
importlib.reload(us8.outputs)
importlib.reload(pulp_solver)

from utils import log, load_adls, save_txt, save_LP_adls, save_csv
from us8.constants import constants
from us8.operations import operations
from us8.solver_operations import solver_operations
from us8.constraints import Constraints
from us8.limits import Limits
from us8.outputs import write_objective_function_coef, define_optimization_results
from pulp_solver import PulpSolver


import df_detailed
import rolling_limits
import norm_limits
import quantile_limits
import constant_limits
import fixed_limits
import custo_real

importlib.reload(df_detailed)
importlib.reload(rolling_limits)
importlib.reload(norm_limits)
importlib.reload(quantile_limits)
importlib.reload(constant_limits)
importlib.reload(fixed_limits)
importlib.reload(custo_real)

from df_detailed import df_detailed
from rolling_limits import rolling_limits
from norm_limits import norm_limits
from quantile_limits import quantile_limits
from constant_limits import constant_limits
from fixed_limits import fixed_limits
from custo_real import custo_real


# dbutils.notebook.run('Modelos-Preditivo', 0)


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
# MAGIC #limits
# MAGIC Limits é uma classe do script modules/limits.py


rolling_limits = Limits.read_limits(rolling_limits) 
quantile_limits = Limits.read_limits(quantile_limits)
constant_limits = Limits.read_limits(constant_limits)
norm_limits = Limits.read_limits(norm_limits)


continue_limits = [
# 'GRAN_OCS_TM@',
'POTE1_I@08MO-MO-821I-'
# 'DENS1_C@08HO-BP-826I-',
# 'ROTA1_I@08FI-FL-827I-'
]

# 'POTE1_I@08FI-BV-827I-',]
# 'GRAN_OCS_TM@08PE-BD-840I-',]


# norm_limits['ROTA1_I@08PR-RP-822I-01M2'] = 'ROTA1_I@08PR-RP-822I-01M1' 
# for k, v in norm_limits.items():
#   if type(norm_limits[k]) == str:
#     norm_limits[k] = norm_limits[norm_limits[k]]


temp_limits = pd.DataFrame(columns = ['Range_max', 'TAG', 'Valor_Real', 'Valor_Norm', 'Ascending'])


# MAGIC %md
# MAGIC #aplica shap


for qualidade in ['compressao', 'relacao gran', 'SE PR', 'umidade', 'SE PP']:
    
  for range_min, range_max in constants.production_range:
    log.info(f'qualidade: {qualidade}, ranges: {range_min}, {range_max}')
    prod_pq = False

    df_train = datasets[qualidade].copy()

    # tag para saber se usina esta produzindo ou nao
    if 'PROD_PQ_Y@08US' not in df_train.columns:
        df_train.insert(loc=0, column='PROD_PQ_Y@08US', value=datasets['gas']['PROD_PQ_Y@08US'])
        prod_pq = True

    df_train = df_train[(df_train['PROD_PQ_Y@08US'] > 0)]

    df_train = df_train.replace([np.inf, -np.inf], np.nan)
    df_train = df_train.fillna(method='bfill').fillna(method='ffill').fillna(method='bfill').fillna(0)

  #         log.info(f'{range_min}, {range_max}')

    status_column = [x for x in df_train.columns if 'status' in x or 'ProducaoPQ_Moagem' in x]
    if len(status_column) > 0:
        df_train.drop(status_column, axis=1, inplace=True)

    df_train_c = df_train[(df_train['PROD_PQ_Y@08US'] >= range_min) & (df_train['PROD_PQ_Y@08US'] <= range_max)].copy()
          
    if prod_pq:
        df_train_c.drop(['PROD_PQ_Y@08US'], axis=1, inplace=True)

    if df_train_c.shape[0] <= 1:
        continue

  #         Ridge.imprime_coef = ridge_imprime_coefqualidade

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
    log.info(df_train_c[df_train_c.columns[:-1]].shape)
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
#               shap_value = operations.unnormalize_feature(scalers, col, y_interp(0.0), 'one_operation')                
#               log.info(col, "{:.2f}".format(y_interp(0.0)), "{:.2f}".format(shap_norm_value), (feat[1][1] < feat[1][len(feat[1])-1]))
        temp_limits = temp_limits.append({'Range_max':range_max, 'TAG':col, 'Valor_Real':y_interp(0.0), 'Valor_Norm': shap_norm_value, 'Ascending':(feat[1][1] < feat[1][len(feat[1])-1])}, ignore_index=True)       


# MAGIC %md
# MAGIC # escreve constraints


def milestone_temp_txt(step):
    '''
    salva arquivos temporarios para tentar identificar quando que determinada restricao é adicionada no arquivo restricoes-faixa.
    step: iterador do arquivo salvo.
    '''
    with open(os.path.join(solver_path, f'restricoes-faixa-{range_min}-{range_max}.txt'), 'r') as temp_txt:
        temp_txt = temp_txt.read() + '\n\n'
        temp_txt += str(len(temp_txt.split('\n')))
        with open(os.path.join(solver_path, f'restricoes-faixa-{range_min}-{range_max}_{step}.txt'), 'w') as save_temp_txt:
            log.info(f'{temp_txt}, file={save_temp_txt}')


def build_restrictions(solver_path, models_coeficients, test_restrictions = False):
    production = datasets['particulados2']['PROD_PQ_Y@08US']
    production_pc = datasets['particulados2']['PROD_PC_I@08US']


    for range_min, range_max in constants.production_range:
    # for range_min, range_max in [(950, 1000)]:
        log.info(f'{range_min},{range_max}')

        with open(os.path.join(solver_path, f'restricoes-faixa-{range_min}-{range_max}.txt'), 'w') as constraint_files:
            # TODO: verificar essa parte, pois depende do millas
            for model in models_results:
                features_coeficient = solver_operations.retrieve_model_coeficients(model, models_results)
                descriptive_args = {'file': constraint_files, 'model_target': model,
                                    'datasets': datasets, 'df_detailed': df_detailed,
                                    'scalers': scalers,
                                    'models_coeficients': models_coeficients,
                                    'features_coeficient': features_coeficient,
                                    'models_results': models_results}


    #                 if model.startswith('dens_moinho'): continue
                models_coeficients = solver_operations.write_descriptive_contraints(**descriptive_args)

            production_query = ((production >= range_min) & (production <= range_max) & (production_pc >= range_min)).copy()
            log.info(f'data size {sum(production_query)}')

            features_limits = {}
    #             log.debug('DEBUG:', production_query)
    #             log.debug(datasets['particulados1'][production_query].shape)
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

                if 'CALC1_Y@08FI-FD00' in feature:
                    feature_in_prod = models_features[feature][production_query]
                    lmin = feature_in_prod.quantile(0.25)
                    lmax = feature_in_prod.quantile(0.9)

                elif feature in ['PESO1_I@08MO-BW-813I-03M1', 'PESO1_I@08MO-BW-813I-04M1']:
                    feature_in_prod = models_features[feature][production_query]
                    lmin = feature_in_prod.quantile(0.8)
                    lmax = feature_in_prod.quantile(0.95)

                elif feature in ['TEMP1_I@08QU-QU-855I-GQA', 'TEMP1_I@08QU-QU-855I-GQB', 'TEMP1_I@08QU-QU-855I-GQC', 'TEMP1_I@08QU-QU-855I-GQ01', 'TEMP1_I@08QU-QU-855I-GQ02']:
                    feature_in_prod = models_features[feature][production_query]
                    lmin = feature_in_prod.quantile(0.4)
                    lmax = feature_in_prod.quantile(0.9)

                elif feature in set(temp_limits.TAG.values) and 'POTE1_I@08FI-BV-827I-' not in feature and 'ROTA1_I@08FI-FL-827I-' not in feature:
                    feature_in_prod = models_features[feature][production_query]
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
                    feature_in_prod = models_features[feature][production_query]
                    lmin = feature_in_prod.quantile(0.3)
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

        operations.replace_string_from_file(solver_path, range_min, range_max)


if not(os.path.exists(solver_path)):
    os.mkdir(solver_path)
    
# salva costs.csv
with open(f'{solver_path}/custo_real.json', 'w') as fp:
    json.dump(custo_real, fp)
write_objective_function_coef(solver_path, scalers)

# constroi restricoes
test_restrictions = False
build_restrictions(solver_path, models_coeficients, test_restrictions = test_restrictions)
if test_restrictions == False:
    diffchecker_sufix = '_standard'
if test_restrictions == True:
    diffchecker_sufix = '_filtered'


# MAGIC %md
# MAGIC # temporarily remove ranges


#%sh
#rm 'us8/Variables - VarX_900-950.csv' 'us8/Variables - VarX_950-1000.csv' 'us8/Variables - VarX_850-900.csv' 'restricoes-faixa-900-950.txt' 'restricoes-faixa-950-1000.txt'  'restricoes-faixa-850-900.txt'


# MAGIC %md
# MAGIC # salva arquivos no datalake para analise (testando)


# analysis_path = f'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/Usina{us_sufix}/Otimizacao/analises'

for i in os.listdir(solver_path):
    if i.startswith('restricoes-faixa'):
        log.info(i)
        with open(solver_path+'/'+i, 'r') as f:
#         with open('us8/'+i, 'r', encoding='ISO-8859-1') as f: # testar com esse encoding
            text = f.read()
            if len(text) == 0: 
                raise AssertionError ("text is empty")
            save_txt(analysis_path, i, text, dbutils)
    if i == 'custo_real.json':
        #read_json() # testar
        with open(solver_path+'/'+i, 'r') as f:
            text = f.read()
            save_txt(analysis_path, i, text, dbutils)

# # salva os restricoes faixa a partir da pasta toda
# dbutils.fs.cp(f'dbfs:/tmp/{solver_path}', analysis_path, True)


# MAGIC %md
# MAGIC # aplica cbc


# solver = PulpSolver(solver_path, os.path.join(solver_path, 'custo_real.json'), constants, us_sufix[1], 'cbc')
solver = PulpSolver(solver_path, os.path.join(solver_path, 'custo_real.json'), 'cbc')


solver.solve_range(ranges=None, us=us_sufix.replace('0', ''))

solver.export_results()


# MAGIC %md
# MAGIC # salva lp


# azure_path = f'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/Usina{us_sufix}/Otimizacao/LPs/'
# save_LP_adls(azure_path, '700-750', solver)
for range_min, range_max in constants.production_range:
    save_LP_adls(azure_path, f'{range_min}-{range_max}', solver, dbutils)
# save_LP_adls(azure_path, '850-900', solver)

# solver.probs['700-750'].writeLP('/dbfs/tmp/us8/lpfiles/700-750.lp')
# # Copiar lpfiles para datalake
# dbutils.fs.cp('dbfs:/tmp/us8/lpfiles/', azure_path, True)


# MAGIC %md
# MAGIC # gera output


df_all = pd.concat(datasets.values(), axis=1)
df_all = df_all.loc[:, ~df_all.columns.duplicated()]

scalers = operations.define_integer_scalers(df_all, scalers)

df_output = define_optimization_results(solver_path, scalers, datasets)


# MAGIC %md
# MAGIC #salva resultados do otimizador


time = datetime.today().strftime('%Y-%m-%d')
log.info(path)
save_csv(path, f'resultado_otimizador-US{us_sufix}_{time}.csv', df_output, dbutils)


# MAGIC %md
# MAGIC #testes


# MAGIC %sh
# MAGIC cat us8/lpfiles/700-750.lp


df_output.loc[df_output['TAG'].str.contains("Consumo_energia")]


df_output.loc[df_output['TAG'].str.contains("dens_moinho")].groupby(by=['TAG', 'faixa']).first()


df_output.loc[df_output['TAG'].str.contains("gas")].groupby(by=['TAG', 'faixa']).first()


df_output.loc[df_output['TAG'].str.contains("'@08QU-GAS'")].groupby(by=['TAG', 'faixa']).first()


df_output.loc[df_output['TAG'].str.contains("GRAN_OCS_TM@08PE-BD")].groupby(by=['TAG', 'faixa']).first()


len(datasets.keys())


# MAGIC %sh
# MAGIC ls /databricks/driver/us8


