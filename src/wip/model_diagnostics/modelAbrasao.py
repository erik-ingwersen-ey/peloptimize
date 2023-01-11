model_name = 'abrasao'
# NUMERO DE DISCOS DE PELOTAMENTO
numDiscos = 12
col_target = 'ABRA_-0,5_PQ_L@08QU'
media_movel = 6


dados = {
        'Processo' : df.index,    
            
#         'bentonita' : ((df['PESO1_I@08MI-LW-832I-01M1'] + df['PESO1_I@08MI-LW-832I-02M1']) / (df['PESO1_I@08MI-BW-832I-01M1'] + df['PESO1_I@08MI-BW-832I-02M1']) * 100 / 0.8693) / df['PROD_PQ_Y@08US'],
        #calc atualizado
        'bentonita' : (df['PESO1_I@08MI-LW-832I-01M1'] + df['PESO1_I@08MI-LW-832I-02M1']) / df['PROD_PQ_Y@08US'] * 1000,

        'TAXA1_I@08MO-BW-813I-03_4M1': df['TAXA1_I@08MO-BW-813I-03_4M1'],

        'QUIM_CAO_PP_L@08PR': df['QUIM_CAO_PP_L@08PR'],

        'QUIM_CFIX_PP_L@08PR': df['QUIM_CFIX_PP_L@08PR'],

        'GRAN_-0,045_PR_L@08FI': df['GRAN_-0,045_PR_L@08FI'],  

        'SUP_SE_PR_L@08FI': df['SUP_SE_PR_L@08FI'],

        'PESO1_I@08PN-TR-860I-09M1': df['PESO1_I@08PN-TR-860I-09M1'],

        'PROD_PQ_Y@08US': df['PROD_PQ_Y@08US'],

        'VELO1_C@08QU-FR-851I-01M1': df['VELO1_C@08QU-FR-851I-01M1'],

        'PRES1_OCS_S@08QU-PF-852I-04M1': df['PRES1_OCS_S@08QU-PF-852I-04M1'],

        'PRES1_C@08QU-PF-852I-06M1': df['PRES1_C@08QU-PF-852I-06M1'],

        'PRES1_C@08QU-VD-853I-17M1': df['PRES1_C@08QU-VD-853I-17M1'],

#         'rel_pressaoArAsc_Desc': df['PRES1_C@08QU-PF-852I-06M1'] / df['PRES1_C@08QU-VD-853I-17M1'],

        'Soma_FUNC1_D_PE-BD-840I': df[[x for x in df.columns if 'FUNC1_D@08PE-BD-840I-' in x]].sum(1),#

        'Soma_PESO1_I_PE-BW-840I-01_12': df[[x for x in df.columns if 'PESO1_I@08PE-BW-840I-' in x]].sum(1),

        'Media_NIVE1_6_QU-FR-851I-01M1': df[[x for x in df.columns if 'NIVE' in x and '@08QU-FR-851I-' in x]].iloc[:, :-1].mean(1),

        'Std_NIVE1_6_QU-FR-851I-01M1': df[[x for x in df.columns if 'NIVE' in x and '@08QU-FR-851I-' in x]].iloc[:, :-1].std(1),
  
        'NIVE7_I@08QU-FR-851I-01M1': df['NIVE7_I@08QU-FR-851I-01M1'],

        'Media_ROTA1_I_PE-BD-840I-01_12': df[[x for x in df.columns if 'ROTA1_I@08PE-BD-840I-' in x]].sum(1)/df[[x for x in df.columns if 'FUNC1_D@08PE-BD-840I-' in x]].sum(1),

        'Std_ROTA1_I_PE-BD-840I-01_12': df[[x for x in df.columns if 'ROTA1_I@08PE-BD-840I-' in x]].std(1),   

        'UMID_H2O_PR_L@08FI': df['UMID_H2O_PR_L@08FI'],

        'VAZA1_I@08QU-ST-855I-01': df['VAZA1_I@08QU-ST-855I-01'], ## Adicionado com base no dicionário de dados.

        'FUNC2_D@08PR-TR-822I-01M1': df['FUNC2_D@08PR-TR-822I-01M1'],
  
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
        'status': 1,
  
  
        col_target: df[col_target]

}

df2 = pd.DataFrame(dados).set_index('Processo')

# concatena rotacao dos ventiladores no dataframe DF2
df2 = pd.concat([df2, df[['ROTA1_I@08QU-PF-852I-02M1', 'ROTA1_I@08QU-PF-852I-03M1', 'ROTA1_I@08QU-PF-852I-04M1', 
                          'ROTA1_I@08QU-PF-852I-05M1', 'ROTA1_I@08QU-PF-852I-06M1', 'ROTA1_I@08QU-PF-852I-08M1']]], axis=1)

# concatena grupos de queima no dataframe DF2
df2 = pd.concat([df2, df[[x for x in df.columns if 'TEMP1_I@08QU-QU-855I-GQ' in x]]], axis=1)


df2 = pd.concat([df2, df[[x for x in df.columns if 'TEMP5_I@08QU-PF-852I-' in x]]], axis=1)

df2 = pd.concat([df2, df[[x for x in df.columns if 'GRAN_OCS_TM@08PE-BD-840I-' in x]]], axis=1)
df2 = pd.concat([df2, df[[x for x in df.columns if 'GRAN_OCS_10-12@08PE-BD-840I-' in x]]], axis=1)
df2 = pd.concat([df2, df[[x for x in df.columns if 'GRAN_OCS_12-16@08PE-BD-840I-' in x]]], axis=1)

for num in np.arange(1,13):
    df2['GRAN_OCS_others_' + str(num).zfill(2)] = df[[x for x in df.columns if ('GRAN_OCS' in x) and ('PE-BD-840I-' + str(num).zfill(2) in x) and not('10-12' in x) and not('12-16' in x) and 'GRAN_OCS_TM' not in x]].sum(axis=1)


# identifica TAGs com valores INF
df2 = df2.replace([-np.inf, np.inf], np.nan)
df2.columns.to_series()[np.isinf(df2).any()]


df2 = df2.fillna(method='ffill').fillna(method='bfill')


#### Limites DF


df2 = df2[df2['PROD_PQ_Y@08US'] >= faixamin]

limite_inf = df2[col_target].mean() - 3* df2[col_target].std()
limite_sup = df2[col_target].mean() + 3* df2[col_target].std()

df2 = df2[(df2[col_target] > limite_inf) & (df2[col_target] < limite_sup)]

df2 = df2[df2['Soma_FUNC1_D_PE-BD-840I'] > 0]
cols = [x for x in df2.columns if 'PRES1_I' in x and 'QU-WB-851I' in x]
for col in cols:
    df2[col] = pd.to_numeric(df2[col], errors='coerce').fillna(value=0)/pd.to_numeric(df2['PROD_PQ_Y@08US'], errors='coerce').fillna(value=0)


### Limites


limits = {
    
#     "bentonita": (None,0.01),
    
    'TAXA1_I@08MO-BW-813I-03_4M1': (1.0, 3.173),
    
    'QUIM_CAO_PP_L@08PR': (1,5, 3.5),
    
    'PRES1_OCS_S@08QU-PF-852I-04M1': (df2['PRES1_OCS_S@08QU-PF-852I-04M1'].quantile(0.003), df2['PRES1_OCS_S@08QU-PF-852I-04M1'].quantile(0.992)),
    
    'PRES1_C@08QU-PF-852I-06M1': (df2['PRES1_C@08QU-PF-852I-06M1'].quantile(0.002), -4),
    
    'PRES1_C@08QU-VD-853I-17M1': (df2['PRES1_C@08QU-VD-853I-17M1'].quantile(0.003), df2['PRES1_C@08QU-VD-853I-17M1'].quantile(0.995)),
    
    'GRAN_-0,045_PR_L@08FI': (df2['GRAN_-0,045_PR_L@08FI'].quantile(0.003), df2['GRAN_-0,045_PR_L@08FI'].quantile(0.999)),       
       
    "ROTA1_I@08QU-PF-852I-01M1": (384.038,None),
    
    "ROTA1_I@08QU-PF-852I-02M1": (None,525.213),
    
    "ROTA1_I@08QU-PF-852I-06M1": (100,393),
    
    "TEMP1_I@08QU-QU-855I-GQ01": (589.630,None),
    
    "TEMP1_I@08QU-QU-855I-GQ03": (677.427,None),
    
    "TEMP1_I@08QU-QU-855I-GQ04": (750,None),
    
    "TEMP1_I@08QU-QU-855I-GQ05": (800,None),
    
    "TEMP1_I@08QU-QU-855I-GQ06": (865.698,None),
    
    "TEMP1_I@08QU-QU-855I-GQ07": (989.791,None),
    
    "TEMP1_I@08QU-QU-855I-GQ08": (1085.647,None),
    
    "TEMP1_I@08QU-QU-855I-GQ09": (1100,None),
    
    "TEMP1_I@08QU-QU-855I-GQ10": (1100,None),
    
    "TEMP1_I@08QU-QU-855I-GQ11": (1200,None),
    
    "TEMP1_I@08QU-QU-855I-GQ12": (1100,None),
    
    "TEMP1_I@08QU-QU-855I-GQ13": (1100,None),
  
#      **{t: (df2[t].quantile(0.12), None) for t in df2[[x for x in df.columns if 'GRAN_OCS_TM@08PE-BD-840I-' in x]]}

    
}


# corta elementos fora dos limites
for k,limit in limits.items():
    print(k, end="")
    if not k in df2.columns: print(' (None)'); continue
    if limit[0] != None:
        print(' Min', limit[0], end="")
        df2 = df2[(df2[k] >= limit[0])]
        
    if limit[1] != None:
        print(' Max', limit[1], end="")
        df2 = df2[(df2[k] <= limit[1])]
        
    print(" ",df2.shape)


# df2 = df2.drop(['PROD_PQ_Y@08US'],axis=1)


# substitui valores INF por zero
infColumns = df2.columns.to_series()[np.isinf(df2).any()]
df2[infColumns] = df2[infColumns].replace(np.inf, 0)
df2.columns.to_series()[np.isinf(df2).any()]


## Melhorando o Modelo


### Filtro outlayer


df2.shape


X = df2.drop(columns=[col_target])
y = df2[col_target]


X_scaled = MinMaxScaler().fit_transform(X)


clf_name = 'knn'
clf = KNN()
clf.fit(X_scaled)


# # seleciona somente os valores que não são outliers
# df2['outliers'] = clf.labels_
# n_outliers = df2[df2.outliers == 1].shape[0]

# # print('numero de outliers: {}'.format(n_outliers))
# df2 = df2[df2.outliers == 0].drop(columns=['outliers'])
# # print('porcentagem do total da base: {}'.format(n_outliers / df2.shape[0] * 100))


df2.shape


## Modelos


# models= {
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
#     },
# }

# results = []
# scalers = {}


# Definindo as bases de treinamento e aplicando um pequeno tratamento de NaN
df_train = df2.copy()
df_train = df_train[df_train.columns.sort_values()]

for t in df_train.columns:
    if t == col_target:
        continue

    df_train[t] = df_train[t].interpolate().fillna(method='bfill').rolling(media_movel, min_periods=1).mean()

#     if t not in scalers:
#         scalers[t] = MinMaxScaler()
#         tmp = df_train[[t]].copy()        
#         scalers[t].fit(tmp.astype(float))

#     df_train[t] = scalers[t].transform(df_train[[t]])


print("ANTES de dropna: {}", df_train.shape)
df_train.dropna(inplace=True, axis=0)
print("DEPOIS de dropna: {}", df_train.shape)


df_train = df_train[[c for c in df_train if c != col_target] 
       + [col_target]]

datasets['abrasao'] = df_train.copy()


#### Modelos


# # Aplicando os modelos nos dados correspondentes
# for method, method_conf in models.items():
#     results += apply_model(method_conf, col_target, method, df_train, df_target)

# print('Fim')


#### Definindo as funções de impressão para cada modelo


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


# # Apresentando o coeficiente do melhor modelo
# print('#####', result['conf'], "#####")
# for k,v in sorted(result['model'].imprime_coef(results=results).items(), key=lambda x:abs(x[1]), reverse = True):
#     print('% .3f' % v, k)


datasets['abrasao'] = df_train.copy()


del df2
del df_train