import .files.limits


# gera algumas variaveis
# IMPORTANTE, além disso, gera a variavel LIMITS, cujo input primário vem do limits_file
# alguns limites são reescalados com log.

import numpy as np

limits = {
    "abrasao": ["LT", 5.5],
    "basicidade": ["GT", 0.95],
    "cfix": ["LTE", 1.26],
    "compressao": ["GT", 290],
    "distribuicao gran": ["GTE", 95],
    "energia_prensa": ["LT", 35],
    "finos": ["LT", 1],
    "taxarp": ["LT", 35],
    "temp_forno":  ('LTE', 180),
    "particulados1": ["LT", 45],
    "particulados2": ["LT", 45],
    "particulados3": ["LT", 45],
    "relacao gran": None,
    "SE PP": None,
    "SUP_SE_PP": None,
    "GRAN PR": None,
    "SE PR": None,
    "gas": None,
    "produtividade filtragem": None,
    "energia_forno": None,
    "energia_moinho": None,
    "custo_GRAN PR": None,
    "custo_SE PR": None,
    "custo_SE PP": None,
    "umidade": None, 
    "torque": None,
    "temp_recirc": None,
    "temp_precipitador_1": None,
    "temp_precipitador_2": None,
    "temp_precipitador_3": None,
    "temp_precipitador_4": None,
    "dens_moinho_1": None,
    "dens_moinho_2": None,
    "dens_moinho_3": None
#     "energia_filtro": None,
}

class constants:
  """ Defining optimizer constants"""

  def __init__(self):
      pass


  @staticmethod
#   def read_limits(file='abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/Usina08/Otimizacao/files/'):
  def _read_limits(limits):      
      data = limits
      # if value is None return None
      # in case value is not none
      # make a tuple of it values
      data = {key: tuple(value) if value else value
                  for key, value in data.items()}
      
#       data.update(to_extend)

      return data


  @staticmethod
  def _scaling_limits(limits, tags):
      """
        Scale tags according to values logs
      """
      for tag in tags:
          limits[tag] = (limits[tag][0], np.log(limits[tag][1]))

      return limits

  @staticmethod
  def define_targets(datasets):
      """
        Return a list of values in the last position,
        which will be considered the
        considered the targets in our models
      """

      return [df.columns[-1] for df in datasets.values()]

#     #nao usado
#   @staticmethod
#   def define_complete_dataset(datasets):
#       """
#         Produce a unique dataset sourced of all datasets
#       """

#       complete_df = pd.DataFrame()

#       for name, dataset in datasets.items():
#           for column in dataset.columns:
#               complete_df[column] = dataset[column]

#       return complete_df

  @staticmethod
  def _additional_features(limits):

      features = {"custo_abrasao": "abrasao", "custo_compressao": "compressao",
                  "custo_distribuicao gran": "distribuicao gran"}

      for feature, value in features.items():
          limits[feature] = limits[value]

      return limits
    
  production_range = [(prod_range, prod_range + 50)
                       for prod_range in range(700, 1000, 50)]

  LIMITS = _read_limits.__func__(limits)
  LIMITS = _additional_features.__func__(LIMITS)
  tags_to_scale = ['compressao', 'particulados1', 'particulados2',
                           'particulados3', 'custo_compressao']
  LIMITS = _scaling_limits.__func__(LIMITS, tags_to_scale)

  # tags that are target in one model
  # and descriptive feature in others
  # old tag_2_var
  _POWER_TOKEN = 'Consumo de Energia (base minério úmido) kWh/ton '
  

  TARGETS_IN_MODEL = {
                        'GANHO PRENSA - US8': 'SE PP',
                        'SUP_SE_PP_L@08PR': 'SUP_SE_PP',
                        'GRAN_-0,045_PR_L@08FI': 'GRAN PR',
                        'SUP_SE_PR_L@08FI': 'SE PR',
                        'VAZA3_I@08QU-ST-855I-01': 'gas',
#                         'QUIM_BAS2_PQ_L@08QU': 'basicidade',
#                         'GRAN_-5_PQ_L@08QU': 'finos',
                        'Consumo de Energia (base minério úmido) kWh/ton': 'energia_moinho',
#                         _POWER_TOKEN + '1': 'energia_moinho1',
#                         _POWER_TOKEN + '2': 'energia_moinho2',
#                         _POWER_TOKEN + '3': 'energia_moinho3',
                        'CONS EE PRENSA - US8': 'energia_prensa',
                        'Calculo da Energia da Filtragem': 'energia_filtro',
                        'CONS ESP VENT TOTAL - US8': 'energia_forno',
                        'TORQ1': 'torque',
                        'TEMP1_I@08QU-HO-851I-01': 'temp_recirc',
                        'QUIM_CFIX_PP_L@08PR': 'cfix',
                        'UMID_H2O_PR_L@08FI': 'umidade',
                        'COMP_MCOMP_PQ_L@08QU': 'compressao',
                        'ABRA_-0,5_PQ_L@08QU': 'abrasao',
                        'PARTIC_I@08QU-CH-854I-01': 'particulados1',
                        'PARTIC_I@08QU-CH-854I-02': 'particulados2',
                        'PARTIC_I@08QU-CH-854I-03': 'particulados3',
                        'RETO1_Y@08PE': 'taxarp',
                        'CALC1_Y@08FI-FD00': 'produtividade filtragem',
                        'TEMP1_I@08QU-PP-871I-01': 'temp_precipitador_1',
                        'TEMP1_I@08QU-PP-871I-02': 'temp_precipitador_2',
                        'TEMP1_I@08QU-PP-871I-03': 'temp_precipitador_3',
                        'TEMP1_I@08PP-DU-872I-01-93': 'temp_precipitador_4',
                        'rel_gran': 'relacao gran',
                        **{'ROTA1_I@08PE-BD-840I-{:02}M1'.format(i): 'rota_disco_' + str(i) for i in range(1, 13)},
                        #**{'GRAN_OCS_TM@08PE-BD-840I-{:02}'.format(i): 'rota_disco_' + str(i) for i in range(1, 13)},
                        **{'DENS1_C@08MO-MO-821I-{:02}'.format(i): 'dens_moinho_' + str(i) for i in range(1, 4)}
                      }

  OBJ_FUNC_COEF = [
#                       'corpo_moedor_especifico_1',
#                       'corpo_moedor_especifico_2',
#                       'corpo_moedor_especifico_3',
                      'corpo_moedor_especifico',
                      'calcario',
                      'antracito',
                      'energia_moinho',
                      'Calculo da Energia da Filtragem',
#                       'energia_moinho1',
#                       'energia_moinho2',
#                       'energia_moinho3',
#                       'POTE1_I@08FI-BV-827I-01M1', 
#                       'POTE1_I@08FI-BV-827I-02M1',
#                       'POTE1_I@08FI-BV-827I-03M1',
#                       'POTE1_I@08FI-BV-827I-04M1',
#                       'POTE1_I@08FI-BV-827I-05RM1',
#                       'POTE1_I@08FI-BV-827I-06M1',
#                       'POTE1_I@08FI-BV-827I-08M1',
#                       'POTE1_I@08FI-BV-827I-09M1',
#                       'POTE1_I@08FI-BV-827I-10RM1',
                      'energia_prensa',
                      'bentonita',
                      'gas',
                      'energia_forno'
                  ]