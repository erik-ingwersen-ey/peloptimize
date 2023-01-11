import .......Utils.predictive_module


import numpy as np
import pandas as pd
import importlib
from sklearn.linear_model import Ridge, Lasso, LassoCV, RidgeCV, ElasticNet, ElasticNetCV
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import KFold, train_test_split, GridSearchCV
from sklearn.pipeline import Pipeline

def mod_filtros(df_train, qualidade, col_target):
    # if qualidade == 'energia_prensa':
    #     df_train = df_train[
    #         [
    #             'POTE1_I@08PR-RP-822I-01M1', 'POTE1_I@08PR-RP-822I-01M2','POTE1_I@08PR-RP-822I-01M1_DIFF', 
    #             'POTE1_I@08PR-RP-822I-01M2_DIFF', 'status','CONS EE PRENSA - US8'
    #         ]
    #     ]

    if qualidade.startswith('cm1'):
        p = df_train['corpo_moedor_especifico_1'] > 0
        df_train = df_train[p]

    elif qualidade == 'torque':
        p = df_train[col_target] >= 3000
        df_train = df_train[p]

    elif qualidade == 'energia_prensa':
        p = df_train[col_target] >= 0.15
        df_train = df_train[p]

    elif qualidade == 'finos':
        p = df_train[col_target] <= 1.5
        df_train = df_train[p]

    elif qualidade == 'compressao':
        p = df_train[col_target] > 200
        df_train = df_train[p]
        
#     elif qualidade == 'produtividade filtragem':
#         continue
        
    else:    
        p = df_train[col_target] > 0
        df_train = df_train[p]
        
    return df_train


def manual_kfold_validation(model, df_train, df_target, **kwargs):
    # log.debug('Entrei no manual_kfold_validation')
    predicted_g = []
    ys = []
    yhats = []
    metrics = []
    indexes = []
    

    params = kwargs.get('kwparams', None)
    
    if params != None:
        cv_thresholds = params['cv_thresholds']
        qualidade = params['qualidade']
        
    else:
        log.info('Nao ha parametro referente ao threshold do cross-validation (cv_thresholds) ou qualidade (qualidade)')
        raise Exception("Nao ha parametro referente ao threshold do cross-validation (cv_thresholds) ou qualidade (qualidade)")
    
    for i in range(len(cv_thresholds)-1):
        

        train = df_train[df_train.index <= cv_thresholds[i]]
        trainy = df_target[df_target.index <= cv_thresholds[i]]

        test = df_train[(df_train.index > cv_thresholds[i]) & (df_train.index <= cv_thresholds[i+1])]
        testy = df_target[(df_target.index > cv_thresholds[i]) & (df_target.index <= cv_thresholds[i+1])]
#         log.debug(f'DEBUG:  {i}')
        if qualidade in ['abrasao','basicidade','distribuicao gran','finos','GRAN PR','SE PR', 'gas', 'energia_moinho', 'energia_moinho1', 'energia_moinho2','energia_moinho3','energia_filtro','energia_forno','energia_prensa','custo_GRAN PR','custo_SE PR','custo_abrasao','custo_distribuicao gran','umidade','torque','temp_recirc','cfix', 'produtividade filtragem', 'temp_forno', 'temp_precipitador_1', 'temp_precipitador_2', 'temp_precipitador_3', 'temp_precipitador_4', 'relacao gran'] or qualidade.startswith('rota_disco_') or qualidade.startswith('cm') or qualidade.startswith('dens_moinho'):
            model.fit(train, trainy)
            p_train= model.predict(train)
            predicted = model.predict(test)

        elif qualidade in ['taxarp','compressao', 'SE PP', 'SUP_SE_PP', 'particulados1', 'particulados2', 'particulados3','custo_compressao','custo_SE PP']:
            model.fit(train, np.log(trainy))
            p_train= np.exp(model.predict(train))
            predicted = np.exp(model.predict(test))

        else:
            assert False, qualidade

        predicted_g=np.concatenate((predicted_g, predicted))

        mse = mean_squared_error(testy, predicted)
        mape = np.mean(np.abs(testy-predicted) / np.abs(testy)) * 100
        r2 = r2_score(testy, predicted)
        r = pd.DataFrame(predicted)[0].corr(pd.DataFrame(testy.values)[0])
        r2_train = r2_score(trainy, p_train)
        n, p = train.shape
        r2_train_adj = 1 - (1 - r2_train) * (n - 1) / (n - p - 1)

        metrics.append((mse, mape, r2, r, r2_train, r2_train_adj))
        
        indexes.extend(testy.index)
        ys.extend(testy)
        yhats.extend(predicted)

    # ---------------------------------------------------------
    # Metrics
    mse = np.mean([metric[0] for metric in metrics])
    mape = np.mean([metric[1] for metric in metrics])
    r2 = np.mean([metric[2] for metric in metrics])
    r = np.mean([metric[3] for metric in metrics])
    r2_train = np.mean([metric[4] for metric in metrics])
    r2_train_adj = np.mean([metric[5] for metric in metrics])

    # log.info(f'final_cross_validation: {qualidade}')
    
    return indexes, ys, yhats, (mse, mape, r2, r, r2_train, r2_train_adj)