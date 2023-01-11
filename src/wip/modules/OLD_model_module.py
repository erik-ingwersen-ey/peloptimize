# import pandas as pd
import numpy as np
import os
import os.path
import matplotlib.pyplot as plt
import pickle
import seaborn as sns
import math
import json
import itertools
from pathlib import Path
from functools import reduce
from sklearn.linear_model import Ridge, Lasso, LassoCV, RidgeCV, ElasticNet, ElasticNetCV
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import KFold, train_test_split, GridSearchCV
from sklearn.pipeline import Pipeline

from IPython.display import HTML
import random

from patsy import dmatrices
from statsmodels.stats.outliers_influence import variance_inflation_factor
import itertools
# from pyod.models.knn import KNN
# from pyod.models.iforest import IForest
# from pyod.utils.data import evaluate_print
# from pyod.utils.utility import standardizer
# from pyod.models.combination import aom, moa, average, maximization


import warnings
warnings.filterwarnings('ignore')

# Auxiliar functions -----------------------------------------------------


def params_to_str(params):
    return ';'.join(
        '{}={}'.format(format_key(k), format_value(v))
        for k, v in params.items()
        if k not in {'random_state', 'seed'}
    )


def format_key(k):
    if '_' in k:
        return ''.join(s[0].upper() for s in k.split('_'))
    return k[:4]


def format_value(v):
    if v is True:
        return 'Y'
    if v is False:
        return 'N'
    if v is None:
        return '-'
    return v


# def hide_toggle(for_next=False):
#     this_cell = """$('div.cell.code_cell.rendered.selected')"""
#     next_cell = this_cell + '.next()'

#     toggle_text = 'Toggle show/hide'  # text shown on toggle link
#     target_cell = this_cell  # target cell to control with toggle
#     # bit of JS to permanently hide code in current cell (only when toggling
#     # next cell)
#     js_hide_current = ''

#     if for_next:
#         target_cell = next_cell
#         toggle_text += ' next cell'
#         js_hide_current = this_cell + '.find("div.input").hide();'

#     js_f_name = 'code_toggle_{}'.format(str(random.randint(1, 2**64)))

#     html = """
#         <script>
#             function {f_name}() {{
#                 {cell_selector}.find('div.input').toggle();
#             }}

#             {js_hide_current}
#         </script>

#         <a href="javascript:{f_name}()">{toggle_text}</a>
#     """.format(
#         f_name=js_f_name,
#         cell_selector=target_cell,
#         js_hide_current=js_hide_current,
#         toggle_text=toggle_text
#     )

#     return HTML(html)


# def data_quality(df):
# # Verificar a qualidade dos dados com relação a NaN e inf

#     print(df.shape)
#     return pd.DataFrame([
#         [i, # Nome do atributo 
#          df[i].isna().sum(), # Quantidade de NaN 
#          (df[i] == np.inf).sum(), # Quantidade de +inf
#          (df[i] == -np.inf).sum()] for i in df.columns], # Quantidade de -inf
#             columns=["Atributo", 'NaN', '+inf', '-inf']
#     ).sort_values(by =['NaN', '+inf', '-inf'], ascending=False)

#* Função que carrega todos os datasets descritos para um dicionário
#? Quaisquer outras alterações sãoo necessárias passar uma função de callback
def load_df(csv_names : dict, callback = None, fread = pd.read_csv, enable_extension=False):
    datasets = {}
    for k,v in csv_names.items(): # iteração sobre todos os dfs
        print("Carregando -> ", k, ':', v, '(dir)' if os.path.isdir(v) else '')
        
        # Chamada recursiva para diretórios
        if os.path.isdir(v):
            filename = None
            if enable_extension: filename = { dirf : os.path.join(v,dirf) for dirf in os.listdir(v)}
            else: filename = { ".".join(dirf.split('.')[:-1]) : os.path.join(v,dirf) for dirf in os.listdir(v)}
                
            res_dict = load_df(filename, callback, fread, enable_extension)
            
            print('Fim do', v, '(dir)\n')
            datasets.update(res_dict)
            
        else:
            # TODO: Checagem que quais os tipos de arquivos permitidos
            datasets[k] = fread(v)
            if callback != None: # Chamando o callback
                datasets[k] = callback(datasets[k])
            
    return datasets

# -------------------------------------------------------------------------------------------------


def show_coef(results):
    # Coeficientes
    best = sorted(results, key=lambda r: -r['metrics']['r2_train'])[0]
    print(best['conf'], best['params'], best['metrics'])
    for i, (a, b) in enumerate(sorted(list(
            zip(best['columns'].tolist(), best['model'].coef_)), key=lambda e: -abs(e[1]))):
        print('{:10.4}  {:}'.format(b, a))

# Impressão dos coeficientes no xgboost


def xgb_imprime_coef(self, results=None):
    return self.get_booster().get_score(importance_type="gain")

# Impressão dos coeficientes no ridge

def ridge_imprime_coef(self, results=None):
    result = {}
    best = sorted(results, key=lambda r: -r['metrics']['r2_train'])[0]
    for a, b in sorted(list(
            zip(best['columns'].tolist(), best['model'].coef_)), key=lambda e: -abs(e[1])):
        result[a] = b

    return result

# -------------------------------------------------------------------------------------------------


def param_combination(model, model_conf, **kwargs):
    model_params = {k: (v if isinstance(v, list) else [
                        v]) for k, v in model['params'].items()}
    model_params['random_state'] = [0]
    keys = sorted(model_params.keys())
    comb_params = [dict(zip(keys, prod)) for prod in itertools.product(
        *(model_params[k] for k in keys))]

    return comb_params


def kfold_validation(model, df_train, df_target, **kwargs):
    predicted_g = []
    ys = []
    yhats = []
    metrics = []
    indexes = []
#     print('DEBUG: ', df_target)
    kfold = KFold(10, shuffle=True, random_state=0)
    for train_ix, test_ix in kfold.split(df_train):
        train, trainy = df_train.iloc[train_ix], df_target.iloc[train_ix]
        test, testy = df_train.iloc[test_ix], df_target.iloc[test_ix]

        model.fit(train, trainy)
        p_train = model.predict(train)
        predicted = model.predict(test)

        predicted_g = np.concatenate((predicted_g, predicted))

        mse = mean_squared_error(testy, predicted)
        mape = np.mean(np.abs(testy - predicted) / np.abs(testy)) * 100
        r2 = r2_score(testy, predicted)
        r = pd.DataFrame(predicted)[0].corr(pd.DataFrame(testy.values)[0])
        r2_train = r2_score(trainy, p_train)
        n, p = train.shape
        r2_train_adj = 1 - (1 - r2_train) * (n - 1) / (n - p - 1)

        metrics.append((mse, mape, r2, r, r2_train, r2_train_adj))

        indexes.extend(testy.index)
        ys.extend(testy)
        yhats.extend(predicted)

    mse = np.mean([metric[0] for metric in metrics])
    mape = np.mean([metric[1] for metric in metrics])
    r2 = np.mean([metric[2] for metric in metrics])
    r = np.mean([metric[3] for metric in metrics])
    r2_train = np.mean([metric[4] for metric in metrics])
    r2_train_adj = np.mean([metric[5] for metric in metrics])

    return indexes, ys, yhats, (mse, mape, r2, r, r2_train, r2_train_adj)

# Mudar kwargs para df


def plot_chart(
        model_conf,
        df_target,
        predicted,
        qualidade,
        params,
        indexes,
        ys,
        yhats,
        metrics, 
        **kwargs):
    mse, mape, r2, r, r2_train, r2_train_adj = metrics

    # Plotting the charts
    graph_title = qualidade + "-" + model_conf  # + '-' + params_to_str(params)
    graph_name_1 = "Graficos\\Line\\" + graph_title + '-line.png'
    graph_name_2 = "Graficos\\Scatter\\" + graph_title + '-scatter.png'
    graph_name_3 = "Graficos\\Scatter-residual\\" + \
        graph_title + '-scatter-residual.png'
    graph_name_4 = "Graficos\\Line-train\\" + graph_title + 'line-train.png'

    graph_title_test = "Teste\n" + graph_title + \
        f"\nMSE {mse:.2f} - MAPE: {mape:.2f} - r2: {r2:.2f}"
    graph_title_train = "Treino\n" + graph_title + \
        f"\nr2: {r2_train:.2f} - r2 adj: {r2_train_adj:.2f}"

    dg = pd.DataFrame({'Processo': indexes, 'Real': ys})
    dg['Predicted'] = yhats
    dg = dg.set_index('Processo')

    lines = dg.plot.line(figsize=(25, 15), fontsize=10, style='.-')
    plt.title(graph_title_test, fontsize=20)
    plt.xlabel('DATA')
    plt.ylabel(qualidade.upper())
#     plt.savefig(graph_name_1)
    plt.show()
    plt.close()

    scatters = dg.plot.scatter(
        x='Real',
        y='Predicted',
        c='DarkBlue',
        figsize=(
            20,
            15))
    plt.title(graph_title_test, fontsize=20)
    plt.axis('equal')
    plt.xlabel('REAL', fontsize=15)
    plt.ylabel('PREDITO', fontsize=15)
    plt.ylim(dg['Real'].min(), dg['Real'].max())
    plt.xlim(dg['Real'].min(), dg['Real'].max())
#     plt.savefig(graph_name_2)
    plt.show()
    plt.close()

    dg['Residual'] = dg['Real'] - dg['Predicted']
    scatters = dg.plot.scatter(
        x='Real',
        y='Residual',
        c='DarkBlue',
        figsize=(
            20,
            15))
    plt.title(graph_title_test, fontsize=20)
    plt.axis('equal')
    plt.xlabel('REAL', fontsize=15)
    plt.ylabel('RESIDUO', fontsize=15)
    plt.ylim(dg['Residual'].min(), dg['Residual'].max())
    plt.xlim(dg['Real'].min(), dg['Real'].max())
#     plt.savefig(graph_name_3)
    plt.show()
    plt.close()

    plt.figure(figsize=(25, 15))
    plt.title(graph_title_train, fontsize=20)
    df_target.plot(style='.-')
    pd.Series(predicted, index=df_target.index).plot(style='.-')
#     plt.savefig(graph_name_4)
    plt.show()
    plt.close()


def apply_model(model, model_name, model_conf, df_train, df_target,
                selfTrain=True,
                parameter_combination=param_combination,
                validation=kfold_validation,
                plot_charts=plot_chart,
                **kwargs):

    print('### ' + model_conf)
    
    # Chaves para cada função
    param_comb  = kwargs.get('param_combination', None)
    param_valid = kwargs.get('param_validation', None)
    param_plot  = kwargs.get('param_plotting', None)
    

    results = []

    comb_params = parameter_combination(model, model_conf, kwparams=param_comb)
    
    for params in comb_params:
        method = model['model'](**params)
        
        

        # Applying the kfold fit model
#         print('DEBUG: ', method, df_train, df_target, param_valid)
        indexes, ys, yhats, metrics = validation(method, df_train, df_target, kwparams=param_valid)
        
        
        
        # Taking the metrics
        mse, mape, r2, r, r2_train, r2_train_adj = metrics
        
        
        # Retrain model with all dataset
        if selfTrain:
            method.fit(df_train, df_target)
            predicted = method.predict(df_train)
            
            
        else: predicted = None

        # plotting charts
        plot_charts(
            model_conf,
            df_target,
            predicted,
            model_name,
            params,
            indexes,
            ys,
            yhats,
            metrics, 
            kwparams=param_plot)
        

        # Appending the best results
        results.append({
            'conf': model_conf,
            'metrics': {
                'mse': mse,
                'mape': mape,
                'r2': r2,
                'r': r,
                'r2_train': r2_train,
                'r2_train_adj': r2_train_adj,
            },
            'model': method,
            'columns': df_train.columns,
            'params': params,
            'indexes': indexes,
            'ys': ys,
            'yhats': yhats,
            'predicted': predicted,
        })
        
#         result_str = '{:.<20} - MSE: {:6.3f} - MAPE: {:6.3f}% - r2: {:6.3f} - r: {:6.3f} - r2_train: {:6.3f} {:6.3f}' # marcelo trocou
        result_str = '{:.<20} - MSE: {:6.3f} - MAPE: {:6.3f}% - r2: {:6.3f} - r: {:6.3f} - r2_train: {:6.3f} - r2_train_adj: {:6.3f}'
        print(
            result_str.format(
                params_to_str(params),
                mse,
                mape,
                r2,
                r,
                r2_train,
                r2_train_adj))

    return results


# TODO: Usar a coluna de threshold para aplicar os limites?
def miss_test(df_limits, df_target, df_predicted, threshold=0.1):
    # Definindo colunas para o target e o predito
    df_limits['target'] = df_target
    df_limits['predic'] = df_predicted

    # Adicionando o threshold para o min e max
    df_limits['min'] = df_limits['min'] - threshold
    df_limits['max'] = df_limits['max'] + threshold

    # Aplicando o teste
    miss = []
    total = 0.0

    for indx, row in df_limits.iterrows():
        #     if row[]
        if (row['target'] >= row['min']) and (row['target'] <= row['max']):
            miss.append(0)
        elif (row['target'] < row['min']) and (row['predic'] < row['min']):
            miss.append(0)
        elif (row['target'] > row['max']) and (row['predic'] > row['max']):
            miss.append(0)
        else:
            miss.append(1)

        if (row['target'] < row['min']) or (row['target'] > row['max']):
            total += 1.0

    df_limits['miss'] = miss

    print("Predicted Miss:", df_limits.miss.sum())
    print("Target Miss:", total)
    # print("Porcentagem:", 100 * df_limits.miss.sum() / df_limits.shape[0])
    print("Porcentagem:", 100 * df_limits.miss.sum() / total)

    return total, df_limits