model_name = 'umidade'
col_target = 'UMID_H2O_PR_L@08FI'
numDiscos = 12
media_movel = 3


## Preprocessamento


# Definicao de variaveis auxiliares

funcs_bp = ['FUNC1_D@08HO-BP-826I-05M1',
 'FUNC1_D@08HO-BP-826I-06RM1',
 'FUNC1_D@08HO-BP-826I-07M1',
 'FUNC1_D@08HO-BP-826I-08RM1']

denss_bp = [
    'DENS1_C@08HO-BP-826I-05',
    'DENS1_C@08HO-BP-826I-06R',
    'DENS1_C@08HO-BP-826I-07',
    'DENS1_C@08HO-BP-826I-08R',
]

rotas_bp = [
    'ROTA1_I@08HO-BP-826I-05M1',
    'ROTA1_I@08HO-BP-826I-06RM1',
    'ROTA1_I@08HO-BP-826I-07M1',
    'ROTA1_I@08HO-BP-826I-08RM1',
]

tags_troca = [x for x in df.columns if 'TIME2_I@08FI-FL-827I-' in x]
tags_troca_l1 = tags_troca[:5]


Contador_1 = (df['PESO1_I@08MO-BW-821I-01M1'] * df['FUNC1_D@08MO-BW-821I-01M1'] > 0).astype(int)
Contador_2 = (df['PESO1_I@08MO-BW-821I-02M1'] * df['FUNC1_D@08MO-BW-821I-02M1'] > 0).astype(int)
Contador_3 = (df['PESO1_I@08MO-BW-821I-03M1'] * df['FUNC1_D@08MO-BW-821I-03M1'] > 0).astype(int)

Soma_Func_Moinho = Contador_1 + Contador_2 + Contador_3


#### Dados


dados = {
    'Processo' : df.index,    
    
#     'Media_ROTA1_I_HO-BP-826I-05_08' : (df[rotas_bp] * df[funcs_bp].rename(columns=dict(zip(funcs_bp, rotas_bp)))).sum(axis=1) / df[funcs_bp].sum(axis=1),#
  
    'Media_ROTA1_I_HO-AG-826I-01_03' : (df['FUNC1_D@08HO-AG-826I-01M1']*df['ROTA1_I@08HO-AG-826I-01M1']+df['FUNC1_D@08HO-AG-826I-02M1']*df['ROTA1_I@08HO-AG-826I-02M1']+df['FUNC1_D@08HO-AG-826I-03M1']*df['ROTA1_I@08HO-AG-826I-03M1'])/(df['FUNC1_D@08HO-AG-826I-01M1']+df['FUNC1_D@08HO-AG-826I-02M1']+df['FUNC1_D@08HO-AG-826I-03M1']),
    'Max_TIME2_I_FI-FL-827I-01_05' : df[tags_troca_l1].max(axis=1),   #max_tags_troca_l1

    'Soma_PESO1_I_MO-BW-821I_01_03' : ((df['PESO1_I@08MO-BW-821I-01M1'] * df['FUNC1_D@08MO-BW-821I-01M1']) + (df['PESO1_I@08MO-BW-821I-02M1'] * df['FUNC1_D@08MO-BW-821I-02M1']) +(df['PESO1_I@08MO-BW-821I-03M1']* df['FUNC1_D@08MO-BW-821I-03M1'])),#

    'PROD_PQ_Y@08US' : df['PROD_PQ_Y@08US'],
  
    'SUP_SE_PR_L@08FI': df['SUP_SE_PR_L@08FI'],  
  
    'Media_ROTA1_I_PE-PN-851I-02-1_2' : (df['ROTA1_I@08PE-PN-851I-02-1']+df['ROTA1_I@08PE-PN-851I-02-2'])/2,   # rotacao peneiras
    
    'Media_GRAN_OCS_TM_PE-BD-840I-01_12': df[[x for x in df.columns if 'GRAN_OCS_TM@08PE-BD-840I-' in x]].sum(1)/ df[[x for x in df.columns if 'FUNC1_D@08PE-BD-840I-' in x]].sum(1), #

    'Media_ROTA1_I_PE-PN-840I-01_12': df[[x for x in df.columns if 'ROTA1_I@08PE-PN-840I-' in x]].sum(1) / df[[x for x in df.columns if 'FUNC1_D@08PE-BD-840I-' in x]].sum(1),
    
    'QUIM_CAO_PP_L@08PR' : df['QUIM_CAO_PP_L@08PR'],   
    
    'GRAN_-2_ANUS8_L@CVMI' : df['GRAN_-2_ANUS8_L@CVMI'],
  
    'Media_NIVE1_I_HO-TQ-826I-03_05' : (df['NIVE1_I@08HO-TQ-826I-03'] * df['FUNC1_D@08HO-AG-826I-01M1']+df['NIVE1_I@08HO-TQ-826I-04']*df['FUNC1_D@08HO-AG-826I-02M1']+df['NIVE1_I@08HO-TQ-826I-05']*df['FUNC1_D@08HO-AG-826I-03M1']) / (df['FUNC1_D@08HO-AG-826I-01M1'] + df['FUNC1_D@08HO-AG-826I-02M1'] + df['FUNC1_D@08HO-AG-826I-03M1']),  #Media_Nivel_Tanque_Homog
  
    'GRAN_-0,045_PR_L@08FI' : df['GRAN_-0,045_PR_L@08FI'],
    'Media_DENS1_C_HO-BP-826I-05_08' : (df[denss_bp] * df[funcs_bp].rename(columns=dict(zip(funcs_bp, denss_bp)))).sum(axis=1) / df[funcs_bp].sum(axis=1),  #denss_bp
#     'Std_DENS1_C_HO-BP-826I-05_08' : df[denss_bp][df[denss_bp] > 1.5].std(axis=1),           # std_densbp
#     'UMID_H2O_ANBUS8_L@CVMI' : df['UMID_H2O_ANBUS8_L@CVMI'],
    'VAZA1_I@08PE-40F32': df['VAZA1_I@08PE-40F32'],  #vazao disco pelotamento
    'PESO2_I@08PA-TR-811I-08M1' : df['PESO2_I@08PA-TR-811I-08M1'],
    'Soma_FUNC1_D@08MO-BW-821I-01_03': Soma_Func_Moinho,
    col_target : df[col_target]
}

df2 = pd.DataFrame(dados).set_index('Processo')

df2 = pd.concat([df2, df[[x for x in df.columns if 'FUNC1_D@08FI-BV-827I-' in x]]], axis=1)


# Inf para NaN
df2.replace([np.inf, -np.inf], np.nan, inplace=True)


## Modelos


# models = {
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


#### Limites


limits = {
    'nives_fl' : (93, None),
    'press_fl' : (None, -0.75),
    'press_so' : (0.5, 2),
    'denss_bp' : (None, 1000),
    'rotas_bp' : (650, None),
    'CALC1_Y@08FI-FD00' : (None, 3.5),
    'vazas1_bv' : (None, 63),
    'UMID_H2O_PR_L@08FI' : (6.5, 9),
    'Soma_Func_Moinho' : (0, None),
    'Media_Rotacao_Britador' : (700, None),
    'PROD_PQ_Y@08US' : (400, None), # min 300,
   
    'Media_Alimentacao_Moinhos': (None, 450),
    'UMID_H2O_ANBUS8_L@CVMI': (None, 30),
    'PESO2_I@08PA-TR-811I-08M1': (1000, 1500),
    'TAXA1_I@08MO-BW-813I-03_4M1': (1.5, 3), # Coisa
   
    'Soma_Func_Bombas_Tanque_Homog': (1, None),
    'NIVE1_I@08MO-SI-813I-06': (60, None),
    'GRAN_-2_ANUS8_L@CVMI': (40, None),
    'QUIM_CFIX_PP_L@08PR': (1, None), # Coisa
    'PESO1_I@08PA-TR-811I-08M1': (None, 2000),
    'PESO2_I@08PA-TR-811I-08M1': (1000,1500),
    'rz_PROD_PQ_Y@08US|qtdDiscosFunc':(45,None),
    'rotacaoPeneiraAvg': (700,None),

}


df2.index.min(), df2.index.max()


# corta elementos fora dos limites
for k,limit in limits.items():
    print(k, end="")
    if not k in df2.columns: print(' (None)'); continue
    if limit[0] != None:
        print(' Min', limit[0], end="")
        df2 = df2[(df2[k] >= float(limit[0]))]
        
    if limit[1] != None:
        print(' Max', limit[1], end="")
        df2 = df2[(df2[k] <= limit[1])]
        
    print("")


#### Restrições


df2.index.min(), df2.index.max()


# Trecho específico para aplicar o modelo
df_model = df2.copy()


df_model = df_model[df_model['PROD_PQ_Y@08US'] >= faixamin]
df_model = df_model[df_model['PROD_PQ_Y@08US'] <= faixamax]


df_model.index.min(), df_model.index.max()


### Aplicando o modelo


# Definindo as bases de treinamento e aplicando um pequeno tratamento de NaN
df_train = df_model.copy()
df_train = df_train[df_train.columns.sort_values()]

for t in df_train.columns:
    if t == col_target:
        continue

    df_train[t] = df_train[t].interpolate().fillna(method='bfill').rolling(media_movel, min_periods=1).mean()#.fillna(method='ffill').fillna(0)

#     if t not in scalers:
#         scalers[t] = MinMaxScaler()
#         tmp = df_train[[t]].copy()        
#         scalers[t].fit(tmp.astype(float))

#     df_train[t] = scalers[t].transform(df_train[[t]])


print("ANTES de dropna: {}", df_train.shape)
df_train.dropna(inplace=True)
print("DEPOIS de dropna: {}", df_train.shape)


# # Separando a base de treino e target
# df_target = df_train[col_target].copy()
# df_train = df_train.drop(col_target, axis=1)


# {text} <h4 id="modelos">Modelos</h4>


# # Aplicando os modelos nos dados correspondentes
# for method, method_conf in models.items():
#     results += apply_model(method_conf, model_name, method, df_train, df_target)

# print('Fim')


#### definindo as funções de impressão


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


# Melhor modelo é aquele com o menor MAPE
# best_one = df_result.sort_values(by=['MAPE']).index[0]


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

datasets['umidade'] = df_train.copy()


del df2
del df_train
