import glob
import pandas as pd

files = []
filenames_list = []
for filename in glob.glob('teste.lp'):
  with open(filename, 'r') as fp:
    filenames_list.append(filename)
    files.append(fp.readlines())


# MAGIC %sh 
# MAGIC 
# MAGIC ls us8/lpfiles


files


dd = pd.DataFrame(files, filenames_list).T


display(dd)


 display(dd.iloc[1000:2000])


display(dd.iloc[2000:3000])


display(dd.iloc[3000:4000])


display(dd.iloc[4000:50000])


display(dd.iloc[5000:60000])


# MAGIC 
# MAGIC %sh
# MAGIC ls


df_output = pd.read_csv('us8/resultado_otimizador-formato-antigo.csv', sep=';')
display(df_output)


df_output.shape


display(pd.read_csv('us8/restricoes-faixa-700-750.txt', sep='\n'))


solver.get_variables()['700-750'].get('Media_ROTA1_I_PE_PN_840I_01_12')


display(pd.read_csv('us8/dados_2021_compressao_understanding.csv'))


# MAGIC %sh
# MAGIC 
# MAGIC mkdir us8/lpfiles


