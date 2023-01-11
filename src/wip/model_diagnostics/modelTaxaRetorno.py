# NUMERO DE DISCOS DE PELOTAMENTO
numDiscos = 12
# col_target = 'taxaRetorno'

model_name = 'retorno'
col_target = 'RETO1_Y@08PE'


# Definicao de variaveis auxiliares
disc2 = df['FUNC1_D@08PE-BD-840I-02M1']+df['FUNC1_D@08PE-BD-840I-04M1']+df['FUNC1_D@08PE-BD-840I-06M1']+df['FUNC1_D@08PE-BD-840I-12M1']

inc2 = df[['POSI2_I@08PE-BD-840I-02', 'POSI2_I@08PE-BD-840I-04', 'POSI2_I@08PE-BD-840I-06','POSI2_I@08PE-BD-840I-12']].sum(1)


dados = {
    'Processo' : df.index,    

#         'bentonita' : (df['PESO1_I@08MI-LW-832I-01M1'] + df['PESO1_I@08MI-LW-832I-02M1']) / (df['PESO1_I@08MI-BW-832I-01M1'] + df['PESO1_I@08MI-BW-832I-02M1']) * 100 / 0.8693,
        
        'bentonita' : (df['PESO1_I@08MI-LW-832I-01M1'] + df['PESO1_I@08MI-LW-832I-02M1']) / df['PROD_PQ_Y@08US'] * 1000,

        'TAXA1_I@08MO-BW-813I-01M1': df['TAXA1_I@08MO-BW-813I-01M1'],                
        
        'Soma_FUNC1_D_PE-BD-840I': df[[x for x in df.columns if 'FUNC1_D@08PE-BD-840I-' in x]].sum(1),
        
        'Soma_PESO1_I_PE-BW-840I-01_12': df[[x for x in df.columns if 'PESO1_I@08PE-BW-840I-' in x]].sum(1),#
  
        'Media_ROTA1_I_PE-BD-840I-01_12': df[[x for x in df.columns if 'ROTA1_I@08PE-BD-840I-' in x]].sum(1)/df[[x for x in df.columns if 'FUNC1_D@08PE-BD-840I-' in x]].sum(1),#                    
        
        'Media_ROTA1_I_PE-PN-840I-01_12': df[[x for x in df.columns if 'ROTA1_I@08PE-PN-840I-' in x]].sum(1)/df[[x for x in df.columns if 'FUNC1_D@08PE-BD-840I-' in x]].sum(1),#                 
        
        'inclinacaoAvg': inc2 / disc2,
        
        'VAZA1_I@08PE-40F32': df['VAZA1_I@08PE-40F32'],
        
        'Soma_VAZA1_I_MI-MI-832I-01_02': df['VAZA1_I@08MI-MI-832I-01']+df['VAZA1_I@08MI-MI-832I-02'],   # vazaomisturadores
        
        'SUP_SE_PR_L@08FI': df['SUP_SE_PR_L@08FI'],
        
        'SUP_SE_PP_L@08PR': df['SUP_SE_PP_L@08PR'],
    
        'UMID_H2O_PR_L@08FI':df['UMID_H2O_PR_L@08FI'],
    
        'QUIM_CAO_PP_L@08PR' : df['QUIM_CAO_PP_L@08PR'],
        
        'PROD_PQ_Y@08US' : df['PROD_PQ_Y@08US'],
  
        **{'ROTA1_I@08PE-PN-840I-{:02}M1'.format(i+1): df['ROTA1_I@08PE-PN-840I-{:02}M1'.format(i+1)] for i in range(0, 12)},
        
        **{'FUNC1_D@08PE-BD-840I-{:02}M1'.format(i): df['FUNC1_D@08PE-BD-840I-{:02}M1'.format(i)]  for i in [2, 4, 6, 12]},
        
    
        **{'POSI2_I@08PE-BD-840I-{:02}'.format(i): df['POSI2_I@08PE-BD-840I-{:02}'.format(i)]  for i in [2, 4, 6, 12]},
        
        col_target: df[col_target]
}

df2 = pd.DataFrame(dados).set_index('Processo')

df2['alim_rot'] = df2['Soma_PESO1_I_PE-BW-840I-01_12']/df2['Media_ROTA1_I_PE-BD-840I-01_12']

df2 = pd.concat([df2, df[[x for x in df.columns if 'GRAN_OCS_TM@08PE-BD-840I-' in x]]], axis=1)
df2 = pd.concat([df2, df[[x for x in df.columns if 'GRAN_OCS_10-12@08PE-BD-840I-' in x]]], axis=1)
df2 = pd.concat([df2, df[[x for x in df.columns if 'GRAN_OCS_12-16@08PE-BD-840I-' in x]]], axis=1)

for num in np.arange(1,13):
    df2['GRAN_OCS_others_' + str(num).zfill(2)] = df[[x for x in df.columns if ('GRAN_OCS' in x) and ('PE-BD-840I-' + str(num).zfill(2) in x) and not('10-12' in x) and not('12-16' in x) and 'GRAN_OCS_TM' not in x]].sum(axis=1)


# Inf para NaN
df2.replace([np.inf, -np.inf], np.nan, inplace=True)


# models = {    
#     ## Parametros iguais aos de Umidade
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


#### Restrições


df2.index.min(), df2.index.max()


df2 = df2[df2['PROD_PQ_Y@08US'] > 500]

df2 = df2[(df2[col_target] >= 15) & (df2[col_target] <= 45)]

df2 = df2[df2['Soma_FUNC1_D_PE-BD-840I'] > 0]


df2.index.min(), df2.index.max()


df_model = df2.copy()


df_model = df_model.drop(['PROD_PQ_Y@08US'],axis=1)


# i=0
limits = {}
for col in df.columns:
    if col not in limits:
        lmin, lmax = df[col].quantile(0.002), df[col].quantile(0.998)        
    


#### Limites


limits = {    
    'TAXA1_I@08MO-BW-813I-01M1' : (1, 8),
    'Soma_PESO1_I_PE-BW-840I-01_12' : (1200, 2000),
    'inclinacaoAvg' : (45.5, 48),
    'umidade': (6.5, 11),
#     **{'GRAN_OCS_TM@08PE-BD-840I-{:02}'.format(i): (df_model['GRAN_OCS_TM@08PE-BD-840I-{:02}'.format(i)].quantile(0.12), None) for i in range(1, 13)}
}


# corta elementos fora dos limites
for k,limit in limits.items():
    print(k, end="\n")
    if not k in df_model.columns: print(' (None)'); continue
    
    print(' Reducao', (df_model.shape[0] - df_model[(df_model[k] >= limit[0])].shape[0]), end="")
    
    if limit[0] != None:
        print(' Min', limit[0], end="")
        df_model = df_model[(df_model[k] >= limit[0])]
        
    if limit[1] != None:
        print(' Max', limit[1], end="")
        df_model = df_model[(df_model[k] <= limit[1])]
        
        
    print("")


# {text} ---


### Melhorando o modelo


#### Tratamento de outlier


df_model = df_model.fillna(method='ffill').fillna(method='bfill')


x, y = df_model.drop(columns=[col_target]), df_model[col_target]


scaler = MinMaxScaler().fit_transform(x)
x = KNN(n_neighbors = 7).fit_predict(scaler)


df_model['out'] = x
df_model = df_model[(df_model['out'] == 0)].drop(columns=['out'])


### Rodando modelo


df_train = df_model.copy()
df_train = df_train[df_train.columns.sort_values()]

for t in df_train.columns:
    if t == col_target:
        continue
    df_train[t] = df_train[t].interpolate().fillna(method='ffill').fillna(method='bfill')
    
#     if t not in scalers:
#         scalers[t] = MinMaxScaler()
#         tmp = df_train[[t]].copy()
#         scalers[t].fit(tmp.astype(float))

#     df_train[t] = scalers[t].transform(df_train[[t]])


print("ANTES de dropna: {}", df_train.shape)
df_train.dropna(inplace=True)
print("DEPOIS de dropna: {}", df_train.shape)


# # Separando a base de treino e target
# df_train = df_train.dropna(subset=[col_target])
# df_target = df_train[col_target].copy()
# df_train = df_train.drop(col_target, axis=1)


# # Aplicando os modelos nos dados correspondentes
# for method, method_conf in models.items():
#     results += apply_model(method_conf, col_target, method, df_train, df_target)

# print('Fim')


#### definindo as funções de impressão para cada modelo


# XGBRegressor.imprime_coef = xgb_imprime_coef


# {text} ---


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
# for k,v in result['model'].imprime_coef(results=results).items():
# #     print(k,v)
#     print('% .3f' % v, k)


### Gravando modelo e scaler


df_train = df_train[[c for c in df_train if c != col_target] 
       + [col_target]]

datasets['taxarp'] = df_train.copy()


del df2
del df_train