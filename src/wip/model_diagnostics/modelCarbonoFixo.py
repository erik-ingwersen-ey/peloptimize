col_target = 'QUIM_CFIX_PP_L@08PR'
model_name = 'cfix'
media_movel = 6


# Definicao de variaveis auxiliares

Contador_1 = (df['PESO1_I@08MO-BW-821I-01M1'] * df['FUNC1_D@08MO-BW-821I-01M1'] > 0).astype(int)
Contador_2 = (df['PESO1_I@08MO-BW-821I-02M1'] * df['FUNC1_D@08MO-BW-821I-02M1'] > 0).astype(int)
Contador_3 = (df['PESO1_I@08MO-BW-821I-03M1'] * df['FUNC1_D@08MO-BW-821I-03M1'] > 0).astype(int)

Soma_Func_Moinho = Contador_1 + Contador_2 + Contador_3


# Definicao das variaveis preditoras

dados = {
    'Processo' : df.index,
    'Media_ROTA1_I_AD-BR-813I-01_02' : (df['FUNC1_D@08AD-BR-813I-01M1']*df['ROTA1_I@08AD-BR-813I-01M1']+df['FUNC1_D@08AD-BR-813I-02M1']*df['ROTA1_I@08AD-BR-813I-02M1'])/(df['FUNC1_D@08AD-BR-813I-01M1']+df['FUNC1_D@08AD-BR-813I-02M1']),

    'Soma_PESO1_I_MO-BW-813I-03_04' : (df['PESO1_I@08MO-BW-813I-03M1'] * df['FUNC1_D@08AD-BR-813I-01M1'] + df['PESO1_I@08MO-BW-813I-04M1'] * df['FUNC1_D@08AD-BR-813I-02M1']),
    
    'Media_ROTA1_I_HO-AG-826I-01_03' : (df['FUNC1_D@08HO-AG-826I-01M1']*df['ROTA1_I@08HO-AG-826I-01M1']+df['FUNC1_D@08HO-AG-826I-02M1']*df['ROTA1_I@08HO-AG-826I-02M1']+df['FUNC1_D@08HO-AG-826I-03M1']*df['ROTA1_I@08HO-AG-826I-03M1'])/(df['FUNC1_D@08HO-AG-826I-01M1']+df['FUNC1_D@08HO-AG-826I-02M1']+df['FUNC1_D@08HO-AG-826I-03M1']),
    
    'Media_PESO1_I_MO-BW-821I-01_03' :  ((df['PESO1_I@08MO-BW-821I-01M1'] * df['FUNC1_D@08MO-BW-821I-01M1']) + (df['PESO1_I@08MO-BW-821I-02M1'] * df['FUNC1_D@08MO-BW-821I-02M1'])+(df['PESO1_I@08MO-BW-821I-03M1']* df['FUNC1_D@08MO-BW-821I-03M1']))/Soma_Func_Moinho,
    
    'Soma_PESO1_I_MO-BW-821I_01_03' : ((df['PESO1_I@08MO-BW-821I-01M1'] * df['FUNC1_D@08MO-BW-821I-01M1']) + (df['PESO1_I@08MO-BW-821I-02M1'] * df['FUNC1_D@08MO-BW-821I-02M1']) +(df['PESO1_I@08MO-BW-821I-03M1']* df['FUNC1_D@08MO-BW-821I-03M1'])),
    
    'Media_NIVE1_I_MO-SI-821I-01_03' : (df['NIVE1_I@08MO-SI-821I-01']*df['FUNC1_D@08MO-BW-821I-01M1']+df['NIVE1_I@08MO-SI-821I-02']*df['FUNC1_D@08MO-BW-821I-02M1']+df['NIVE1_I@08MO-SI-821I-03']*df['FUNC1_D@08MO-BW-821I-03M1'])/Soma_Func_Moinho,

    'Media_NIVE1_I_MO-TQ-821I-01_03' : ((df['NIVE1_I@08MO-TQ-821I-01'] * df['FUNC1_D@08MO-BW-821I-01M1']) + (df['NIVE1_I@08MO-TQ-821I-02']* df['FUNC1_D@08MO-BW-821I-02M1'])+ (df['NIVE1_I@08MO-TQ-821I-03']* df['FUNC1_D@08MO-BW-821I-03M1']))/Soma_Func_Moinho,

    'Media_NIVE1_I_HO-TQ-826I-03_05' : (df['NIVE1_I@08HO-TQ-826I-03'] * df['FUNC1_D@08HO-AG-826I-01M1']+df['NIVE1_I@08HO-TQ-826I-04']*df['FUNC1_D@08HO-AG-826I-02M1']+df['NIVE1_I@08HO-TQ-826I-05']*df['FUNC1_D@08HO-AG-826I-03M1']) / (df['FUNC1_D@08HO-AG-826I-01M1'] + df['FUNC1_D@08HO-AG-826I-02M1'] + df['FUNC1_D@08HO-AG-826I-03M1']),


    'Soma_PESO1_I_MO-BW-813I-03_04' : (df['PESO1_I@08MO-BW-813I-03M1'] + df['PESO1_I@08MO-BW-813I-04M1']),

#     'VM Balsa' : df['VAZA1_I@08AP-BP-875I-01']*df['DENS1_I@08AP-TQ-875I-03'],
    
#     'GRAN_-2_ANUS8_L@CVMI' : df['GRAN_-2_ANUS8_L@CVMI'],
#     'UMID_H2O_ANBUS8_L@CVMI' : df['UMID_H2O_ANBUS8_L@CVMI'],
    'FUNC2_D@08MO-TR-813I-06M1' : df['FUNC2_D@08MO-TR-813I-06M1'],
    'FUNC1_D@08PA-TR-811I-08M1' : df['FUNC1_D@08PA-TR-811I-08M1'],
    'PESO2_I@08PA-TR-811I-08M1' : df['PESO2_I@08PA-TR-811I-08M1'],
    'NIVE1_I@08MO-SI-811I-01' : df['NIVE1_I@08MO-SI-811I-01'],
    'NIVE1_I@08MO-SI-813I-06' : df['NIVE1_I@08MO-SI-813I-06'],
    'PROD_PQ_Y@08US' : df['PROD_PQ_Y@08US'],
    
    # Soma FUNCs 
#     'Soma_FUNC1_D_HO-BP-826I-05_08' : df['FUNC1_D@08HO-BP-826I-05M1']+df['FUNC1_D@08HO-BP-826I-06RM1']+df['FUNC1_D@08HO-BP-826I-07M1']+df['FUNC1_D@08HO-BP-826I-08RM1'],
#     'Soma_FUNC1_D_HO-AG-826I-01_03' : (df['FUNC1_D@08HO-AG-826I-01M1']+df['FUNC1_D@08HO-AG-826I-02M1']+df['FUNC1_D@08HO-AG-826I-03M1']),
    'Soma_FUNC1_D_MO-BW-821I-01_03' : Soma_Func_Moinho,
    
    # Tags da abrasão    
    'Soma_PESO1_I_PE-BW-840I-01_12': df[[x for x in df.columns if 'PESO1_I@08PE-BW-840I-' in x]].sum(1),
    'Media_NIVE1_6_QU-FR-851I-01M1': df[[x for x in df.columns if 'NIVE' in x and '@08QU-FR-851I-' in x]].iloc[:, :-1].mean(1),   
    'NIVE7_I@08QU-FR-851I-01M1': df['NIVE7_I@08QU-FR-851I-01M1'],    
    'UMID_H2O_PR_L@08FI': df['UMID_H2O_PR_L@08FI'],
    'PRES1_C@08QU-PF-852I-06M1': df['PRES1_C@08QU-PF-852I-06M1'],
    'PRES1_C@08QU-VD-853I-17M1': df['PRES1_C@08QU-VD-853I-17M1'], 
    'PRES1_C@08QU-PF-852I-01M1': df['PRES1_C@08QU-PF-852I-01M1'],
    
    # GasProd
    'gasNatural_Prod': df['VAZA1_I@08QU-ST-855I-01'] / df['PROD_PQ_Y@08US'],
    
    # Temp Recirc & Dens Bomb
    'TEMP1_I@08QU-HO-851I-01': df['TEMP1_I@08QU-HO-851I-01'],
    'Max_PESO2_I_MO-BW-813I-03_04': df[['PESO2_I@08MO-BW-813I-03M1', 'PESO2_I@08MO-BW-813I-04M1']].max(axis=1),   ## antracitoCorrecao
    'TAXA1_I@08MO-BW-813I-03_4M1': df['TAXA1_I@08MO-BW-813I-03_4M1'],
    'TAXA1_I@08MO-BW-813I-01M1': df['TAXA1_I@08MO-BW-813I-01M1'],
    **{'NIVE1_I@08MO-SI-821I-{:02}'.format(i) :df['NIVE1_I@08MO-SI-821I-{:02}'.format(i)] for i in range(1, 4)},
    **{'FUNC1_D@08MO-BW-821I-{:02}M1'.format(i) :df['FUNC1_D@08MO-BW-821I-{:02}M1'.format(i)] for i in range(1, 4)},
    col_target: df[col_target]
}

df2 = pd.DataFrame(dados).set_index('Processo')


# Inf para NaN
df2.replace([np.inf, -np.inf], np.nan, inplace=True)


## Modelos


# models = {
#     'xgboost' : {
#         'model' : XGBRegressor,
#         'params' : {
#             'base_score': 0.5,
#             'colsample_bytree': 0.3, 
#             'gamma': 0, 
#             'importance_type': 'gain', 
#             'learning_rate': 0.1, 
#             'max_delta_step': 0, 
#             'max_depth': 5, 
#             'min_child_weight': 1, 
#             'n_estimators': 100,
#             'objective': 'reg:squarederror', 
#             'scale_pos_weight': 1, 
#             'alpha': 0.1
#         }    
#     },
# }

# results = []
# scalers = {}


#### Restrições


df2.index.min(), df2.index.max()


df2 = df2[df2['Soma_FUNC1_D_MO-BW-821I-01_03'] > 0]
df2 = df2[df2['Media_ROTA1_I_AD-BR-813I-01_02'] >= 700]
df2 = df2[df2['PROD_PQ_Y@08US'] >= 300]


df2.index.min(), df2.index.max()


df_model = df2.copy()


filter_less = {
    'Media_PESO1_I_MO-BW-821I-01_03': 450, 
#     'UMID_H2O_ANBUS8_L@CVMI': 30,
    'PESO2_I@08PA-TR-811I-08M1': 1500,
    'TAXA1_I@08MO-BW-813I-03_4M1': 3
}

filter_bigger = {
#     'Media_NIVE1_I_HO-TQ-826I-03_05': 100, 
#     'Soma_FUNC1_D_HO-BP-826I-05_08': 1.5,             
    'NIVE1_I@08MO-SI-813I-06': 60, 
    'PROD_PQ_Y@08US': 600,
    'PESO2_I@08PA-TR-811I-08M1': 1000, 
    'NIVE1_I@08MO-SI-813I-06': 60,
    'TAXA1_I@08MO-BW-813I-03_4M1': 1.5,
#     'GRAN_-2_ANUS8_L@CVMI': 40,
    'QUIM_CFIX_PP_L@08PR': 1
}

filters = {'Less': filter_less, 'Bigger': filter_bigger}

for operation, data_filter in filters.items():
    for key, value in data_filter.items():
        
        if operation == 'Less':
            df_model = df_model[df_model[key] <= value]
        else: # Bigger
            df_model = df_model[df_model[key] >= value]


df_model.index.min(), df_model.index.max()


### Aplicando o modelo


# Definindo as bases de treinamento e aplicando um pequeno tratamento de NaN
df_train = df_model.copy()
df_train = df_train[df_train.columns.sort_values()]

for t in df_train.columns:
    if t == col_target:
        continue

    df_train[t] = df_train[t].interpolate().fillna(method='bfill').rolling(media_movel, min_periods=1).median()

#     if t not in scalers:
#         scalers[t] = MinMaxScaler()
#         tmp = df_train[[t]].copy()        
#         scalers[t].fit(tmp.astype(float))

#     df_train[t] = scalers[t].transform(df_train[[t]])


# Aplicando tecnica de remoção de outlier
outlier_remove = False

if outlier_remove:
    aux = df_train.drop(columns=[col_target])
    aux = pd.DataFrame(MinMaxScaler().fit_transform(aux), columns=aux.columns.to_list())

    clf = KNN()
    clf.fit(aux)
    X = pd.concat([
            df_train.reset_index(), pd.DataFrame(clf.labels_, columns=['outlier'])], 
        axis=1).set_index('Processo')

    df_train = X[(X['outlier'] == 0)].drop(['outlier'], axis=1)


print("ANTES de dropna: {}", df_train.shape)
df_train.dropna(inplace=True)
print("DEPOIS de dropna: {}", df_train.shape)


df_train = df_train[[c for c in df_train if c != col_target] 
       + [col_target]]
datasets['cfix'] = df_train.copy()


# # Separando a base de treino e target
# df_target = df_train[col_target].copy()
# df_train = df_train.drop(col_target, axis=1)


#### modelos 


# # Aplicando os modelos nos dados correspondentes
# for method, method_conf in models.items():
# #     results += modm.apply_model(method_conf, model_name, method, df_train, df_target)
#     results += apply_model(method_conf, model_name, method, df_train, df_target)

# print('Fim')


#### definindo as funções de impressão para cada modelo


# XGBRegressor.imprime_coef = xgb_imprime_coef


# {text} ---


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


##### Impressão dos coeficientes


# result = results[best_one]
# # Apresentando o coeficiente do melhor modelo
# print('#####', result['conf'], "#####")
# for k,v in result['model'].imprime_coef().items():
#     print('%.3f' % v, k)


### Gravando modelo e scaler


# path = 'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/Usina08/retreino/'
# save_model_adls(path, 'model_cfix_ref.joblib', results[best_one]['model'])
# save_model_adls(path, 'scalers_cfix_ref.joblib', scalers)


del df2
del df_train