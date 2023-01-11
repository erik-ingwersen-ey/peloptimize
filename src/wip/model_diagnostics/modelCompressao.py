model_name = 'compressao'
col_target = 'COMP_MCOMP_PQ_L@08QU'
# NUMERO DE DISCOS DE PELOTAMENTO
numDiscos = 12
media_model = 6


umidade = df['UMID_H2O_PR_L@08FI']
rotacaoDiscoAvg = df[[x for x in df.columns if 'ROTA' in x and 'BD-840' in x]].sum(1)/df[[x for x in df.columns if 'FUNC1_D@08PE-BD-840I-' in x]].sum(1)
pressaoForno = df['PRES1_OCS_S@08QU-PF-852I-04M1']
rzUmiRotDiscAvg = umidade / rotacaoDiscoAvg
rzUmiPressaoForno = umidade / pressaoForno


#### Dados


dados = {
    'Processo' : df.index,    
            
#     'bentonita' : (df['PESO1_I@08MI-LW-832I-01M1'] + df['PESO1_I@08MI-LW-832I-02M1']) / (df['PESO1_I@08MI-BW-832I-01M1'] + df['PESO1_I@08MI-BW-832I-02M1']) * 100 / 0.8693,
  
    'bentonita' : (df['PESO1_I@08MI-LW-832I-01M1'] + df['PESO1_I@08MI-LW-832I-02M1']) / df['PROD_PQ_Y@08US'] * 1000,
    'TAXA1_I@08MO-BW-813I-03_4M1': df['TAXA1_I@08MO-BW-813I-03_4M1'],
    'QUIM_CAO_PP_L@08PR': df['QUIM_CAO_PP_L@08PR'],
    'QUIM_CFIX_PP_L@08PR': df['QUIM_CFIX_PP_L@08PR'],

    'GRAN_-0,045_PR_L@08FI': df['GRAN_-0,045_PR_L@08FI'],
    'PESO1_I@08PN-TR-860I-09M1': df['PESO1_I@08PN-TR-860I-09M1'],
    'PROD_PQ_Y@08US': df['PROD_PQ_Y@08US'],

    'VELO1_C@08QU-FR-851I-01M1': df['VELO1_C@08QU-FR-851I-01M1'],

    'PRES1_OCS_S@08QU-PF-852I-04M1': df['PRES1_OCS_S@08QU-PF-852I-04M1'],

    'PRES1_C@08QU-PF-852I-06M1': df['PRES1_C@08QU-PF-852I-06M1'],
    'PRES1_C@08QU-VD-853I-17M1': df['PRES1_C@08QU-VD-853I-17M1'],
    'PRES1_C@08QU-PF-852I-01M1': df['PRES1_C@08QU-PF-852I-01M1'],
    
    'Soma_PESO1_I_PE-BW-840I-01_12': df[[x for x in df.columns if 'PESO1_I@08PE-BW-840I-' in x]].sum(1), #
     
    'Media_NIVE1_6_QU-FR-851I-01M1': df[[x for x in df.columns if 'NIVE' in x and '@08QU-FR-851I-' in x]].iloc[:, :-1].mean(1),
    'Std_NIVE1_6_QU-FR-851I-01M1': df[[x for x in df.columns if 'NIVE' in x and '@08QU-FR-851I-' in x]].iloc[:, :-1].std(1),
        
    'Std_ROTA1_I_PE-BD-840I-01_12': df[[x for x in df.columns if 'ROTA1_I@08PE-BD-840I-' in x]].std(1),
    
    'UMID_H2O_PR_L@08FI': df['UMID_H2O_PR_L@08FI'],
    'rzUmiRotDiscAvg': rzUmiRotDiscAvg,
    'rzUmiPressaoForno': rzUmiPressaoForno,
    
    'VAZA1_I@08QU-ST-855I-01': df['VAZA1_I@08QU-ST-855I-01'],

    'ROTA1_I@08QU-PF-852I-01M1': df['ROTA1_I@08QU-PF-852I-01M1'],  # ventilador resfriamento 52S26
  
    'ROTA1_I@08QU-PF-852I-07M1': df['ROTA1_I@08QU-PF-852I-07M1'],  # ventilador resfriamento 52S31
  
    'Media_PRES1_I_QU-WB-851I-01_03A': df[[
                            'PRES1_I@08QU-WB-851I-01', 
                            'PRES1_I@08QU-WB-851I-02',
                            'PRES1_I@08QU-WB-851I-03A'
                    ]].mean(axis=1),
    'Media_PRES1_I_QU-WB-851I-04_08': df[[
                            'PRES1_I@08QU-WB-851I-04', 'PRES1_I@08QU-WB-851I-05',
                            'PRES1_I@08QU-WB-851I-06', 'PRES1_I@08QU-WB-851I-07', 
                            'PRES1_I@08QU-WB-851I-08'
                    ]].mean(axis=1),
    'Media_PRES1_I_QU-WB-851I-09_13': df[[
                            'PRES1_I@08QU-WB-851I-09', 'PRES1_I@08QU-WB-851I-10', 
                            'PRES1_I@08QU-WB-851I-11', 'PRES1_I@08QU-WB-851I-12', 
                            'PRES1_I@08QU-WB-851I-13'
                    ]].mean(axis=1),
    'PRES1_I@08QU-WB-851I-21': df['PRES1_I@08QU-WB-851I-21'],
    'PRES1_I@08QU-WB-851I-27': df['PRES1_I@08QU-WB-851I-27'],
    'PRES1_I@08QU-WB-851I-31': df['PRES1_I@08QU-WB-851I-31'],

    'SUP_SE_PR_L@08FI': df['SUP_SE_PR_L@08FI'],
#     'Soma_FUNC1_D_PE-BD-840I' : df[[x for x in df.columns if 'FUNC1_D@08PE-BD-840I-' in x]].sum(1),
    
    'QUIM_SIO2_PP_L@08PR' : df['QUIM_SIO2_PP_L@08PR'],
    col_target: df[col_target],
}

df2 = pd.DataFrame(dados).set_index('Processo')

# concatena rotacao dos vetiladores no dataframe DF2
df2 = pd.concat([df2, df[['ROTA1_I@08QU-PF-852I-02M1', 'ROTA1_I@08QU-PF-852I-03M1', 'ROTA1_I@08QU-PF-852I-04M1', 
                          'ROTA1_I@08QU-PF-852I-05M1', 'ROTA1_I@08QU-PF-852I-06M1', 'ROTA1_I@08QU-PF-852I-08M1']]], axis=1)

df2 = pd.concat([df2, df[[x for x in df.columns if 'TEMP1_I@08QU-QU-855I-GQ' in x]]], axis=1)
# df2.drop(columns='TEMP1_I@08QU-QU-855I-GQ06', axis=1, inplace=True)

df2 = pd.concat([df2, df[[x for x in df.columns if 'GRAN_OCS_TM@08PE-BD-840I-' in x]]], axis=1)
df2 = pd.concat([df2, df[[x for x in df.columns if 'GRAN_OCS_10-12@08PE-BD-840I-' in x]]], axis=1)
df2 = pd.concat([df2, df[[x for x in df.columns if 'GRAN_OCS_12-16@08PE-BD-840I-' in x]]], axis=1)

for num in np.arange(1,13):
    df2['GRAN_OCS_others_' + str(num).zfill(2)] = df[[x for x in df.columns if ('GRAN_OCS' in x) and ('PE-BD-840I-' + str(num).zfill(2) in x) and not('10-12' in x) and not('12-16' in x) and 'GRAN_OCS_TM' not in x]].sum(axis=1)       


cols = [x for x in df2.columns if 'PRES1_I' in x and 'QU-WB-851I' in x]
for col in cols:
    df2[col] = pd.to_numeric(df2[col], errors='coerce').fillna(value=0)/pd.to_numeric(df2['PROD_PQ_Y@08US'], errors='coerce').fillna(value=0)


## Modelos


# models = {   
#     ### Parametros obtidos usando HyperOPT (codigo do Joao)
#     'xgboost' : {
#         'model' : XGBRegressor,
#         'params' : {'objective': 'reg:squarederror',
#          'base_score': 0.5,
#          'colsample_bylevel': 1,
#          'colsample_bynode': 1,
#          'colsample_bytree': 1,
#          'gamma': 0,
#          'importance_type': 'gain', 
#          'learning_rate': 0.300000012,
#          'max_delta_step': 0,
#          'max_depth': 6,
#          'min_child_weight': 1,
#          'n_estimators': 100,
#          'n_jobs': 0,
#          'num_parallel_tree': 1,
#          'random_state': 0,
#          'reg_alpha': 0,
#          'reg_lambda': 1,
#          'scale_pos_weight': 1,
#          'subsample': 1,
#          'validate_parameters': False,
#          'verbosity': 1}    
#             },
# }

# results = []
# scalers = {}


### Tratando os dados


# identifica TAGs com valores INF
df2 = df2.replace([-np.inf, np.inf], np.nan)
df2.columns.to_series()[np.isinf(df2).any()]


df2 = df2.fillna(method='ffill').fillna(method='bfill').fillna(0)


df2 = df2[df2['PROD_PQ_Y@08US'] >= faixamin]


#### Limites


limits = {
 'antracito': (1.5, 2.65),
 'calcario': (2.0, 3.05),
 'cfix': (1.01, 1.6440799999999989),
 'granPR': (58.51599999999999, 84.97543999999996),
#  'vel1': (40.0, 700.0),
 'PRES1_OCS_S@08QU-PF-852I-04M1': (-600.0, -220.084942),
 'PRES1_C@08QU-PF-852I-06M1': (-1000.0, 0.0),
 'PRES1_C@08QU-VD-853I-17M1': (-608.407134, -204.80251200000038),
 'PRES1_C@08QU-PF-852I-01M1': (-1000.0, 0.0),
 'Soma_PESO1_I_PE-BW-840I-01_12': (880.61456, 1615.729514),
 'altCamTotal': (400.0, 468.235),
 'Std_NIVE1_6_QU-FR-851I-01M1': (4.55, 20.0),
 'Std_ROTA1_I_PE-BD-840I-01_12': (0.0592931171454093, 3.13579598847637),
 'umidade': (6.375920000000001, 9.125699999999991),
 'rzUmiRotDiscAvg': (1.0084638313999714, 1.5289588372820813),
 'rzUmiPressaoForno': (-0.03842182469565217, -0.011975569149213008),
 'gasNatural': (4000, 10000),
 'se_pr': (1431.4305, 1878.0),
#  **{t: (df2[t].quantile(0.12), None) for t in df2[[x for x in df.columns if 'GRAN_OCS_TM@08PE-BD-840I-' in x]]}

}


df_model = df2.copy()


# corta elementos fora dos limites
for k,limit in limits.items():
    print(k, end="")
    if not k in df_model.columns: print(' (None)'); continue
    if limit[0] != None:
        print(' Min', limit[0], end="")
        df_model = df_model[(df_model[k] >= limit[0])]
        
    if limit[1] != None:
        print(' Max', limit[1], end="")
        df_model = df_model[(df_model[k] <= limit[1])]
        
    print(" ",df_model.shape)

df_model = df_model.drop(['PROD_PQ_Y@08US'],axis=1)


### Detecção de outliers usando pyod


df_model = df_model.replace([np.inf, -np.inf], np.nan).fillna(method='bfill');
# df_model = multi_model_outlier_classification(df_model.drop(col_target, axis=1))
# df_model[col_target] = df.loc[df_model.index, col_target]


### Aplicando o modelo


# Definindo as bases de treinamento e aplicando um pequeno tratamento de NaN
df_train = df_model.copy()
df_train = df_train[df_train.columns.sort_values()]

for t in df_train.columns:
    if t == col_target:
        continue

    df_train[t] = df_train[t].interpolate().fillna(method='ffill').fillna(method='bfill').rolling(media_model, min_periods=1).mean()#.fillna(method='ffill').fillna(0)

#     if t not in scalers:
#         scalers[t] = MinMaxScaler()
#         tmp = df_train[[t]].copy()        
#         scalers[t].fit(tmp.astype(float))

#     df_train[t] = scalers[t].transform(df_train[[t]])


print("ANTES de dropna: {}", df_train.shape)
df_train.dropna(inplace=True)
print("DEPOIS de dropna: {}", df_train.shape)


# Separando a base de treino e target
# df_target = df_train[col_target].copy()
# df_train = df_train.drop(col_target, axis=1)


# {text} <h4 id="modelos">Modelos</h4>


# # Aplicando os modelos nos dados correspondentes
# for method, method_conf in models.items():
#     results += apply_model(method_conf, model_name, method, df_train, df_target)

# print('Fim')


#### definindo as funções de impressão para cada modelo


# XGBRegressor.imprime_coef = xgb_imprime_coef


### Buscando melhor modelo


# # Tomando todos os resultados e transformando em dataframe
# df_result = pd.DataFrame(
#         [(i['conf'], i['model'].get_params().__str__(),
#         i['metricas']['mse'],
#         i['metricas']['mape'],
#         i['metricas']['r2'],
#         i['metricas']['r'],
#         i['metricas']['r2_train'],
#         i['metricas']['r2_train_adj']) for i in results], 

#         # Métricas utilizadas
#         columns=['Modelo', 'Params', 'MSE','MAPE', 'R2', 'R', 'R2 Train', 'R2 Train Adj'])


# # Melhor modelo é aquele com o menor MAPE
# best_one = df_result.sort_values(by=['MAPE']).index[0]


# # Pegando os limites da usina para analisar o target e a predição
# result = results[best_one]


#### Impressão dos coeficientes


# # Apresentando o coeficiente do melhor modelo
# print('#####', result['conf'], "#####")
# for k,v in result['model'].imprime_coef(results=results).items():
# #     print(k,v)
#     print('% .3f' % v, k)


#### Serializando o modelo


df_train = df_train[[c for c in df_train if c != col_target] 
       + [col_target]]

datasets['compressao'] = df_train.copy()


del df2
del df_train