# import os
# import os.path
# import datetime
# import joblib

from pyod.models.knn import KNN

# import pytz
# import numpy as np
# import pandas as pd
# import matplotlib.pyplot as plt

# from xgboost import XGBRegressor

# from sklearn.preprocessing import StandardScaler, MinMaxScaler


#%run ../../Utils/sql_module


#%run ../../../Utils/model_module


def multi_model_outlier_classification(df, outliers_fraction=0.15, n_jobs= -1):
    from pyod.models.abod import ABOD
    from pyod.models.cblof import CBLOF
    from pyod.models.feature_bagging import FeatureBagging
    from pyod.models.hbos import HBOS
    from pyod.models.iforest import IForest
    from pyod.models.knn import KNN
    from pyod.models.lof import LOF
    random_state = np.random.RandomState(42)
    outliers_fraction = 0.15

    # Define seven outlier detection tools to be compared
    classifiers = {
            'Angle-based Outlier Detector (ABOD)': ABOD(contamination=outliers_fraction,),
            'Cluster-based Local Outlier Factor (CBLOF)':CBLOF(contamination=outliers_fraction,check_estimator=False,
                                                               random_state=random_state, n_jobs= n_jobs),
            'Feature Bagging':FeatureBagging(LOF(n_neighbors=35),contamination=outliers_fraction,
                                             check_estimator=False,random_state=random_state, n_jobs= n_jobs ),
            'Histogram-base Outlier Detection (HBOS)': HBOS(contamination=outliers_fraction ),
            'Isolation Forest': IForest(contamination=outliers_fraction,random_state=random_state, n_jobs= n_jobs),
            'K Nearest Neighbors (KNN)': KNN(contamination=outliers_fraction, n_jobs= n_jobs),
            'Average KNN': KNN(method='mean',contamination=outliers_fraction, n_jobs= n_jobs )
    }

    dfx = df.copy()    ### cópia do dataframe contendo os dados
    dfx['outlier'] = 0 
    for i, (clf_name, clf) in enumerate(classifiers.items()):
        print('runing:', clf_name)
        clf.fit(dfx)
        # prediction of a datapoint category outlier or inlier
        y_pred = clf.predict(dfx)
        # copy of dataframe
        dfx['outlier'] += y_pred.tolist()
        print(clf_name, 'ok!')
    limite_minimo_outlier = 0

    #outliers = dfx[dfx.outlier > limite_minimo_outlier].index
    dfx.outlier = dfx.outlier.apply(lambda x: 1 if x > limite_minimo_outlier else 0)
    dfx=dfx[dfx['outlier']==0]
    dfx=dfx.drop(['outlier'], axis=1)
    #dfx.outlier = dfx.outlier * (dfx[pred_var] + 0.001)
    
    return dfx


# # DICIONARIO MODELOS
# listaModelos = ['Carbono Fixo', 'Umidade', 'Taxa de Retorno', 'Abrasão', 'Compressao']
# listaIDModelo = list(range(1,6))
# dicModelos = dict(zip(listaModelos, listaIDModelo))
# print(dicModelos)

# # DICIONARIO Usinas
# listaUsinas = ['Usina 01', 'Usina 02', 'Usina 03', 'Usina 04', 'Usina 05', 'Usina 06', 'Usina 07', 'Usina 08']
# listaIDUsinas = list(range(1,9))
# dicUsinas = dict(zip(listaUsinas, listaIDUsinas))
# print(dicUsinas)


# dicParametros = {'usina': 8}
# plotDebug = True

# LER FAIXA
faixamin = 700
faixamax = 1000
df_faixa = pd.DataFrame({'faixaMin':list(range(faixamin, faixamax, 50)), 'faixaMax':list(range(faixamin + 50, faixamax + 50, 50))})


# tags formato pims
tagpims = [
 'QUIM_CFIX_PR_L@08FI',
 'RETO1_Y@08PE',
 'COMP_MCOMP_PQ_L@08QU',
 'ABRA_-0,5_PQ_L@08QU',
 'DENS1_C@08HO-BP-826I-05',
 'DENS1_C@08HO-BP-826I-06R',
 'DENS1_C@08HO-BP-826I-07',
 'DENS1_C@08HO-BP-826I-08R',
 'DENS1_I@08AP-TQ-875I-03',
 'FUNC1_D@08AD-BR-813I-01M1',
 'FUNC1_D@08AD-BR-813I-02M1',
 'FUNC1_D@08HO-AG-826I-01M1',
 'FUNC1_D@08HO-AG-826I-02M1',
 'FUNC1_D@08HO-AG-826I-03M1',
 'FUNC1_D@08HO-BP-826I-05M1',
 'FUNC1_D@08HO-BP-826I-06RM1',
 'FUNC1_D@08HO-BP-826I-07M1',
 'FUNC1_D@08HO-BP-826I-08RM1',
 'FUNC1_D@08MO-BW-821I-01M1',
 'FUNC1_D@08MO-BW-821I-02M1',
 'FUNC1_D@08MO-BW-821I-03M1',
 'FUNC1_D@08PA-TR-811I-08M1',
 'FUNC1_D@08PE-BD-840I-01M1',
 'FUNC1_D@08PE-BD-840I-02M1',
 'FUNC1_D@08PE-BD-840I-03M1',
 'FUNC1_D@08PE-BD-840I-04M1',
 'FUNC1_D@08PE-BD-840I-05M1',
 'FUNC1_D@08PE-BD-840I-06M1',
 'FUNC1_D@08PE-BD-840I-07M1',
 'FUNC1_D@08PE-BD-840I-08M1',
 'FUNC1_D@08PE-BD-840I-09M1',
 'FUNC1_D@08PE-BD-840I-10M1',
 'FUNC1_D@08PE-BD-840I-11M1',
 'FUNC1_D@08PE-BD-840I-12M1',
 'FUNC2_D@08MO-TR-813I-06M1',
 'FUNC2_D@08PR-TR-822I-01M1',
 'GRAN_-0,045_PR_L@08FI',
 'GRAN_-2_ANUS8_L@CVMI',
 'GRAN_OCS_+16@08PE-BD-840I-01',
 'GRAN_OCS_+16@08PE-BD-840I-02',
 'GRAN_OCS_+16@08PE-BD-840I-03',
 'GRAN_OCS_+16@08PE-BD-840I-04',
 'GRAN_OCS_+16@08PE-BD-840I-05',
 'GRAN_OCS_+16@08PE-BD-840I-06',
 'GRAN_OCS_+16@08PE-BD-840I-07',
 'GRAN_OCS_+16@08PE-BD-840I-08',
 'GRAN_OCS_+16@08PE-BD-840I-09',
 'GRAN_OCS_+16@08PE-BD-840I-10',
 'GRAN_OCS_+16@08PE-BD-840I-11',
 'GRAN_OCS_+16@08PE-BD-840I-12',
 'GRAN_OCS_10-12@08PE-BD-840I-01',
 'GRAN_OCS_10-12@08PE-BD-840I-02',
 'GRAN_OCS_10-12@08PE-BD-840I-03',
 'GRAN_OCS_10-12@08PE-BD-840I-04',
 'GRAN_OCS_10-12@08PE-BD-840I-05',
 'GRAN_OCS_10-12@08PE-BD-840I-06',
 'GRAN_OCS_10-12@08PE-BD-840I-07',
 'GRAN_OCS_10-12@08PE-BD-840I-08',
 'GRAN_OCS_10-12@08PE-BD-840I-09',
 'GRAN_OCS_10-12@08PE-BD-840I-10',
 'GRAN_OCS_10-12@08PE-BD-840I-11',
 'GRAN_OCS_10-12@08PE-BD-840I-12',
 'GRAN_OCS_10-16@08PE-BD-840I-12',
 'GRAN_OCS_12-16@08PE-BD-840I-01',
 'GRAN_OCS_12-16@08PE-BD-840I-02',
 'GRAN_OCS_12-16@08PE-BD-840I-03',
 'GRAN_OCS_12-16@08PE-BD-840I-04',
 'GRAN_OCS_12-16@08PE-BD-840I-05',
 'GRAN_OCS_12-16@08PE-BD-840I-06',
 'GRAN_OCS_12-16@08PE-BD-840I-07',
 'GRAN_OCS_12-16@08PE-BD-840I-08',
 'GRAN_OCS_12-16@08PE-BD-840I-09',
 'GRAN_OCS_12-16@08PE-BD-840I-10',
 'GRAN_OCS_12-16@08PE-BD-840I-11',
 'GRAN_OCS_12-16@08PE-BD-840I-12',
 'GRAN_OCS_16-18@08PE-BD-840I-01',
 'GRAN_OCS_16-18@08PE-BD-840I-02',
 'GRAN_OCS_16-18@08PE-BD-840I-03',
 'GRAN_OCS_16-18@08PE-BD-840I-04',
 'GRAN_OCS_16-18@08PE-BD-840I-05',
 'GRAN_OCS_16-18@08PE-BD-840I-06',
 'GRAN_OCS_16-18@08PE-BD-840I-07',
 'GRAN_OCS_16-18@08PE-BD-840I-08',
 'GRAN_OCS_16-18@08PE-BD-840I-09',
 'GRAN_OCS_16-18@08PE-BD-840I-10',
 'GRAN_OCS_16-18@08PE-BD-840I-11',
 'GRAN_OCS_16-18@08PE-BD-840I-12',
 'GRAN_OCS_5-8@08PE-BD-840I-12',
 'GRAN_OCS_8-10@08PE-BD-840I-01',
 'GRAN_OCS_8-10@08PE-BD-840I-02',
 'GRAN_OCS_8-10@08PE-BD-840I-03',
 'GRAN_OCS_8-10@08PE-BD-840I-04',
 'GRAN_OCS_8-10@08PE-BD-840I-05',
 'GRAN_OCS_8-10@08PE-BD-840I-06',
 'GRAN_OCS_8-10@08PE-BD-840I-07',
 'GRAN_OCS_8-10@08PE-BD-840I-08',
 'GRAN_OCS_8-10@08PE-BD-840I-09',
 'GRAN_OCS_8-10@08PE-BD-840I-10',
 'GRAN_OCS_8-10@08PE-BD-840I-11',
 'GRAN_OCS_8-10@08PE-BD-840I-12',
 'GRAN_OCS_TM@08PE-BD-840I-01',
 'GRAN_OCS_TM@08PE-BD-840I-02',
 'GRAN_OCS_TM@08PE-BD-840I-03',
 'GRAN_OCS_TM@08PE-BD-840I-04',
 'GRAN_OCS_TM@08PE-BD-840I-05',
 'GRAN_OCS_TM@08PE-BD-840I-06',
 'GRAN_OCS_TM@08PE-BD-840I-07',
 'GRAN_OCS_TM@08PE-BD-840I-08',
 'GRAN_OCS_TM@08PE-BD-840I-09',
 'GRAN_OCS_TM@08PE-BD-840I-10',
 'GRAN_OCS_TM@08PE-BD-840I-11',
 'GRAN_OCS_TM@08PE-BD-840I-12',
 'NIVE1_C@08QU-FR-851I-01M1',
 'NIVE1_I@08HO-TQ-826I-03',
 'NIVE1_I@08HO-TQ-826I-04',
 'NIVE1_I@08HO-TQ-826I-05',
 'NIVE1_I@08MO-SI-811I-01',
 'NIVE1_I@08MO-SI-813I-06',
 'NIVE1_I@08MO-SI-821I-01',
 'NIVE1_I@08MO-SI-821I-02',
 'NIVE1_I@08MO-SI-821I-03',
 'NIVE1_I@08MO-TQ-821I-01',
 'NIVE1_I@08MO-TQ-821I-02',
 'NIVE1_I@08MO-TQ-821I-03',
 'NIVE2_I@08QU-FR-851I-01M1',
 'NIVE3_I@08QU-FR-851I-01M1',
 'NIVE4_I@08QU-FR-851I-01M1',
 'NIVE5_I@08QU-FR-851I-01M1',
 'NIVE6_I@08QU-FR-851I-01M1',
 'NIVE7_I@08QU-FR-851I-01M1',
 'PESO1_I@08MI-BW-832I-01M1',
 'PESO1_I@08MI-BW-832I-02M1',
 'PESO1_I@08MI-LW-832I-01M1',
 'PESO1_I@08MI-LW-832I-02M1',
 'PESO1_I@08MO-BW-813I-03M1',
 'PESO1_I@08MO-BW-813I-04M1',
 'PESO1_I@08MO-BW-821I-01M1',
 'PESO1_I@08MO-BW-821I-02M1',
 'PESO1_I@08MO-BW-821I-03M1',
 'PESO1_I@08PE-BW-840I-01M1',
 'PESO1_I@08PE-BW-840I-02M1',
 'PESO1_I@08PE-BW-840I-03M1',
 'PESO1_I@08PE-BW-840I-04M1',
 'PESO1_I@08PE-BW-840I-05M1',
 'PESO1_I@08PE-BW-840I-06M1',
 'PESO1_I@08PE-BW-840I-07M1',
 'PESO1_I@08PE-BW-840I-08M1',
 'PESO1_I@08PE-BW-840I-09M1',
 'PESO1_I@08PE-BW-840I-10M1',
 'PESO1_I@08PE-BW-840I-11M1',
 'PESO1_I@08PE-BW-840I-12M1',
 'PESO1_I@08PN-TR-860I-09M1',
 'PESO2_I@08MO-BW-813I-03M1',
 'PESO2_I@08MO-BW-813I-04M1',
 'PESO2_I@08PA-TR-811I-08M1',
 'POSI2_I@08PE-BD-840I-02',
 'POSI2_I@08PE-BD-840I-04',
 'POSI2_I@08PE-BD-840I-06',
 'POSI2_I@08PE-BD-840I-12',
 'PRES1_C@08QU-PF-852I-01M1',
 'PRES1_C@08QU-PF-852I-06M1',
 'PRES1_C@08QU-VD-853I-17M1',
 'PRES1_I@08QU-WB-851I-01',
 'PRES1_I@08QU-WB-851I-02',
 'PRES1_I@08QU-WB-851I-03A',
 'PRES1_I@08QU-WB-851I-04',
 'PRES1_I@08QU-WB-851I-05',
 'PRES1_I@08QU-WB-851I-06',
 'PRES1_I@08QU-WB-851I-07',
 'PRES1_I@08QU-WB-851I-08',
 'PRES1_I@08QU-WB-851I-09',
 'PRES1_I@08QU-WB-851I-10',
 'PRES1_I@08QU-WB-851I-11',
 'PRES1_I@08QU-WB-851I-12',
 'PRES1_I@08QU-WB-851I-13',
 'PRES1_I@08QU-WB-851I-21',
 'PRES1_I@08QU-WB-851I-27',
 'PRES1_I@08QU-WB-851I-31',
 'PRES1_OCS_S@08QU-PF-852I-04M1',
 'PROD_PQ_Y@07US',
 'PROD_PQ_Y@08US',
 'QUIM_CAO_PP_L@08PR',
 'QUIM_CFIX_PP_L@08PR',
 'QUIM_SIO2_PP_L@08PR',
 'ROTA1_I@08AD-BR-813I-01M1',
 'ROTA1_I@08AD-BR-813I-02M1',
 'ROTA1_I@08HO-AG-826I-01M1',
 'ROTA1_I@08HO-AG-826I-02M1',
 'ROTA1_I@08HO-AG-826I-03M1',
 'ROTA1_I@08HO-BP-826I-05M1',
 'ROTA1_I@08HO-BP-826I-06RM1',
 'ROTA1_I@08HO-BP-826I-07M1',
 'ROTA1_I@08HO-BP-826I-08RM1',
 'ROTA1_I@08PE-BD-840I-01M1',
 'ROTA1_I@08PE-BD-840I-02M1',
 'ROTA1_I@08PE-BD-840I-03M1',
 'ROTA1_I@08PE-BD-840I-04M1',
 'ROTA1_I@08PE-BD-840I-05M1',
 'ROTA1_I@08PE-BD-840I-06M1',
 'ROTA1_I@08PE-BD-840I-07M1',
 'ROTA1_I@08PE-BD-840I-08M1',
 'ROTA1_I@08PE-BD-840I-09M1',
 'ROTA1_I@08PE-BD-840I-10M1',
 'ROTA1_I@08PE-BD-840I-11M1',
 'ROTA1_I@08PE-BD-840I-12M1',
 'ROTA1_I@08PE-PN-840I-01M1',
 'ROTA1_I@08PE-PN-840I-02M1',
 'ROTA1_I@08PE-PN-840I-03M1',
 'ROTA1_I@08PE-PN-840I-04M1',
 'ROTA1_I@08PE-PN-840I-05M1',
 'ROTA1_I@08PE-PN-840I-06M1',
 'ROTA1_I@08PE-PN-840I-07M1',
 'ROTA1_I@08PE-PN-840I-08M1',
 'ROTA1_I@08PE-PN-840I-09M1',
 'ROTA1_I@08PE-PN-840I-10M1',
 'ROTA1_I@08PE-PN-840I-11M1',
 'ROTA1_I@08PE-PN-840I-12M1',
 'ROTA1_I@08PE-PN-851I-02-1',
 'ROTA1_I@08PE-PN-851I-02-2',
 'ROTA1_I@08QU-PF-852I-01M1',
 'ROTA1_I@08QU-PF-852I-02M1',
 'ROTA1_I@08QU-PF-852I-03M1',
 'ROTA1_I@08QU-PF-852I-04M1',
 'ROTA1_I@08QU-PF-852I-05M1',
 'ROTA1_I@08QU-PF-852I-06M1',
 'ROTA1_I@08QU-PF-852I-07M1',
 'ROTA1_I@08QU-PF-852I-08M1',
 'SUP_SE_PP_L@08PR',
 'SUP_SE_PR_L@08FI',
 'TAXA1_I@08MO-BW-813I-01M1',
 'TAXA1_I@08MO-BW-813I-03_4M1',
 'TEMP1_I@08QU-HO-851I-01',
 'TEMP1_I@08QU-QU-855I-GQ01',
 'TEMP1_I@08QU-QU-855I-GQ03',
 'TEMP1_I@08QU-QU-855I-GQ04',
 'TEMP1_I@08QU-QU-855I-GQ05',
 'TEMP1_I@08QU-QU-855I-GQ06',
 'TEMP1_I@08QU-QU-855I-GQ07',
 'TEMP1_I@08QU-QU-855I-GQ08',
 'TEMP1_I@08QU-QU-855I-GQ09',
 'TEMP1_I@08QU-QU-855I-GQ10',
 'TEMP1_I@08QU-QU-855I-GQ11',
 'TEMP1_I@08QU-QU-855I-GQ12',
 'TEMP1_I@08QU-QU-855I-GQ13',
 'TEMP1_I@08QU-QU-855I-GQ14',
 'TEMP1_I@08QU-QU-855I-GQ15',
 'TEMP1_I@08QU-QU-855I-GQ16',
 'TEMP5_I@08QU-PF-852I-01M1',
 'TEMP5_I@08QU-PF-852I-02M1',
 'TEMP5_I@08QU-PF-852I-03M1',
 'TEMP5_I@08QU-PF-852I-04M1',
 'TEMP5_I@08QU-PF-852I-05M1',
 'TEMP5_I@08QU-PF-852I-07M1',
 'TEMP5_I@08QU-PF-852I-08M1',
 'TIME2_I@08FI-FL-827I-01',
 'TIME2_I@08FI-FL-827I-02',
 'TIME2_I@08FI-FL-827I-03',
 'TIME2_I@08FI-FL-827I-04',
 'TIME2_I@08FI-FL-827I-05R',
 'UMID_H2O_ANBUS8_L@CVMI',
 'UMID_H2O_PR_L@08FI',
 'VAZA1_I@08AP-BP-875I-01',
 'VAZA1_I@08MI-MI-832I-01',
 'VAZA1_I@08MI-MI-832I-02',
 'VAZA1_I@08PE-40F32',
 'VAZA1_I@08QU-ST-855I-01',
 'VELO1_C@08QU-FR-851I-01M1',
'FUNC1_D@08FI-BV-827I-01M1', 'FUNC1_D@08FI-BV-827I-02M1', 'FUNC1_D@08FI-BV-827I-03M1', 'FUNC1_D@08FI-BV-827I-04M1', 'FUNC1_D@08FI-BV-827I-05RM1', 'FUNC1_D@08FI-BV-827I-06M1', 'FUNC1_D@08FI-BV-827I-07M1', 'FUNC1_D@08FI-BV-827I-08M1', 'FUNC1_D@08FI-BV-827I-09M1', 'FUNC1_D@08FI-BV-827I-10RM1'
]


# tagdl = tagPIMStoDataLake(tagpims)


# taglist = pd.DataFrame({'tagdl': tagdl, 'tagpims': tagpims})


# dicTags = dict(zip(taglist.tagdl, taglist.tagpims))


# tini, tfim, ultima_previsao = getMinPredDate(dicParametros['usina'])
# tini = datetime.datetime(2019, 1, 1, 0, 0, 0)
# tfim = datetime.datetime(2020, 8, 10, 0, 0, 0)
# ultima_previsao = datetime.datetime(2020, 2, 1, 0, 0, 0)   # nao afeta a modelagem


# tz = pytz.timezone('America/Sao_Paulo')
# tini = datetime.datetime.now(tz)
# tini = tini - datetime.timedelta(days=730)  # dois anos
# tini = tini.replace(microsecond=0, second=0, minute=0)

# tfim = datetime.datetime.now(tz)
# tfim = tfim.replace(microsecond=0, second=0, minute=0)


# tini = tini.strftime('%Y-%m-%d %H:%M:%S')
# # tfim = tfim.strftime('%Y-%m-%d %H:%M:%S')
# print(tini, "\n", tfim)


# df = getFVar(dicParametros['usina'], tini, tfim, taglist)


# df = df.dropna()


# df = df.drop_duplicates(subset=['data', 'variavel'], keep="first")


# df = df.pivot(index = 'data', columns = 'variavel', values = 'valor')


# df.index.min(), df.index.max()


# tini = datetime.datetime.strptime(tini, '%Y-%m-%d %H:%M:%S')


# df = getTAGsDelay(df)


# df = df.drop_duplicates(keep=False)


# substitui valores default=99999 por NaN
# df = df.replace(99999, np.nan)


df.index.min(), df.index.max()


if len(df) > 0:
  print("PROD_PQ_Y = " + str(df.tail(1)['PROD_PQ_Y@08US']))


# df.shape


# df[(df['PROD_PQ_Y@08US'] >= faixamin) & (df['PROD_PQ_Y@08US'] <= faixamax)].shape


# MAGIC %md
# MAGIC ##Executa os modelos de Diagnostico


import modelAbrasao


import modelCarbonoFixo


import modelCompressao


import modelTaxaRetorno


import modelUmidade


