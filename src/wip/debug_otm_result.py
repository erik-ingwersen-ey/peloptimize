import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

plt.rcParams['figure.figsize'] = 14, 8


# MAGIC %sh
# MAGIC ls


df_sql = pd.read_csv('us8/data_dumps/df_sql.csv')
df_sql.set_index('data', inplace=True)
df_sql = df_sql.replace(9999, np.nan).fillna(df_sql.median())
df_otm_result = pd.read_csv('us8/resultado_otimizador-formato-antigo.csv', sep=';', decimal=',')


loc_tags = [x for x in df_sql.columns if re.match('TEMP1_I@08QU-QU-855I-GQ\d+', x)]


production_range = [(prod_range, prod_range + 50)
                       for prod_range in range(700, 1000, 50)]


df_sql


df_otm_result.loc[(df_otm_result['faixa'] == range_str) & (df_otm_result['TAG'] == tag), ['minimo', 'maximo', 'valor real']].values[0]


(df_sql['PROD_PC_I@08US'] < fmax)


df_sql


for fmin, fmax in production_range:
    range_str = '{}-{}'.format(fmin, fmax)
    production_query = ((df_sql['PROD_PQ_Y@08US'] >= fmin ) & (df_sql['PROD_PQ_Y@08US'] < fmax) & (df_sql['PROD_PC_I@08US'] >= fmin))
    df_plot = df_sql.loc[production_query, loc_tags]
    
    for tag in loc_tags:
        minimo, maximo, recommended_value = df_otm_result.loc[(df_otm_result['faixa'] == range_str) & (df_otm_result['TAG'] == tag), ['minimo', 'maximo', 'valor real']].values[0]
        percentile_value = df_plot[tag].values.searchsorted(recommended_value)/len(df_plot[tag])*100
        percentile_value = round(percentile_value, 2)
        
        fig, axs = plt.subplots(1, 2)
        df_plot[tag].hist(bins=100, ax=axs[0])
        label = 'recommended value on percentile: {}% ({.2f})'.format(percentile_value, recommended_value)
        axs[0].axvline(recommended_value, color='red', label=label)
        axs[0].axvline(minimo, color='green', label='minimo', linestyle='--')
        axs[0].axvline(maximo, color='green', label='maximo', linestyle='--')

        axs[0].legend()
        sns.boxplot(y=df_plot[tag], ax=axs[1])
        axs[1].axhline(recommended_value, color='red', label=label)
        
        axs[1].legend()
        plt.suptitle('Recomendação para {} no range {}'.format(tag, range_str), fontsize=20)
        plt.show()
        
        print(end='\n\n')
        


df_sql['FUNC1_D@08QU-FR-851I-01M1'].describe(np.arange(0, 1.05, 0.05))


df_sql['PROD_PC_I@08US'].describe(np.arange(0, 1.05, 0.05))


