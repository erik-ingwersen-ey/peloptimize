import .files.complex_constraints


import .files.general_constraints


import .files.range_complex_constraints


import .files.range_constraints


import .files.variable_constraints


""" Definning the problem constraints. """
import numpy as np
import importlib

class Constraints:
  """ Definning the problem constraints. """
  @staticmethod
  def write_feature_constraints(feature, file, lmin, lmax):

      solver_operations.write_constraint(file, feature+"_limit_min",
                                          [(feature, 1), -lmin, ('GTE', 0)])
      solver_operations.write_constraint(file, feature+"_limit_max",
                                              [(feature, 1), -lmax, ('LTE', 0)])

  @staticmethod
  def read_constraints(file='general_constraints.json', parse=True):
      
      base_path = 'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/Usina08/Otimizacao/files/'
      
      
      data = operations.read_json_dls(base_path, file)
     
      if not parse:
          return data

      # if value is list return tuple
      # in case value is not list
      # return the value
      new_data = {}
      for key, value in data.items():
          new_data[key] = [tuple(operation) if isinstance(operation, list) else operation for operation in value]

      return new_data
  @staticmethod
  def parse_data(data):
      new_data = {}
      for key, value in data.items():
          new_data[key] = [tuple(operation) if isinstance(operation, list) else operation for operation in value]
            
      return new_data
    
    
  @staticmethod
  def write_simple_constraints(file):
      """
         Write constraints that are constant and each range
      """
      new_data = Constraints.parse_data(general_constraints)
      for constraint, operation in new_data.items():
          solver_operations.write_constraint(file, constraint, operation)

  @staticmethod
  def write_targets_limits(file, datasets, features_limits):
    """
        Write constraints to the targets of all models
        removing those that are already defined
    """

    targets = list(map(lambda x: datasets[x].columns[-1], datasets))

    targets_write = list(filter(lambda x: x not in features_limits, targets))

    select_dataset = {target: list(filter(lambda x: target in datasets[x], datasets)) for target in set(targets_write)}

    for target in set(targets_write):        
        values = datasets[select_dataset[target][0]][target]

        if target == '0':

            continue
                
#         if target in 'COMP_MCOMP_PQ_L@08QU':            
            
#             Constraints.write_feature_constraints(target, file, 270, max(values))

#             Constraints.write_feature_constraints("compressao", file, 270, max(values))
        
#         elif target in 'QUIM_CFIX_PP_L@08PR':              
#             write_feature_constraints(target, file, min(values[values > 0]), 1.22)

#             write_feature_constraints("cfix", file, min(values[values > 0]), 1.22)

#         elif target in 'SE PP':
#             Constraints.write_feature_constraints(target, file, min(values[values > 200]), max(values[values < 230]))

#             Constraints.write_feature_constraints("GANHO PRENSA - US8", file, min(values[values > 200]), max(values[values < 230]))

        if target in constants.TARGETS_IN_MODEL.keys():
            new_target = constants.TARGETS_IN_MODEL[target]

            Constraints.write_feature_constraints(new_target, file, min(values), max(values))
            Constraints.write_feature_constraints(target, file, min(values), max(values))

        else:
            Constraints.write_feature_constraints(target, file, min(values), max(values))
            
  @staticmethod
  def define_range_terms(range_terms, scalers):

      range_constraints = solver_operations.define_range_constraints


      if not isinstance(range_terms, list):
          range_terms = [range_terms]


      all_terms = []

      for terms in range_terms:

          step = 1
          if 'step' in terms.keys():
              step = terms['step']

          parsed_terms = range_constraints(terms['feature'], terms['start'], terms['end'] + 1, step)

          norm_features = []
          if 'norm_feature' in terms.keys():
              norm_features = range_constraints(terms['norm_feature'], terms['start'], terms['end'] + 1, step)

          term = terms.copy()

          for index, feature in enumerate(parsed_terms):

              term['feature'] = feature

              if norm_features:
                  _, new_coef = Constraints.measure_new_coef(term, scalers, norm_features[index])
              else:
                  _, new_coef = Constraints.measure_new_coef(term, scalers)


              all_terms.append((feature, new_coef))

      return all_terms

  @staticmethod
  def write_simple_range_terms(file, scalers, features_limits):

#       range_terms = Constraints.read_constraints("range_constraints.json", False)
      range_terms = range_constraints

      for constraint_name, terms in range_terms.items():

          terms = Constraints.parse_complex_constraints(terms, features_limits, scalers)

          solver_operations.write_constraint(file, constraint_name, terms)

  @staticmethod
  def define_ordinary_constraints(terms):

      return [(term['feature'], term['coef']) for term in terms]

  @staticmethod
  def parse_complex_constraints(terms, features_limits, scalers, range_min=None, range_max=None):

      simple_constraint = []

      for term in terms: # each term is a map

          if len(term.keys()) == 2 and "feature" in term.keys() and "coef" in term.keys(): # it is a simple constraint
              simple_constraint.append(tuple(term.values()))

          elif "limit" not in term.keys() and "operator" in term.keys(): # it is a operator - It has only one operator

              operator = tuple(term.values())

          elif "limit" in term.keys() and "operator" not in term.keys():

              limit = Constraints.define_term_limit(term, features_limits, range_min, range_max)
              simple_constraint.append((term["feature"], limit))

          elif "limit" in term.keys() and "operator" in term.keys():

              if term['operator'] == "norm":
                  method_operator = operations.normalize_feature

              limit = Constraints.define_term_limit(term, features_limits, range_min, range_max)

              simple_constraint.append((term["coef"] * method_operator(scalers,
                                                                       term['feature'], limit)))

          elif "start" in term.keys() and "end" in term.keys():

              simple_constraint.extend(Constraints.define_range_terms(term, scalers))


      simple_constraint.append(operator)

      return simple_constraint

  @staticmethod
  def define_term_limit(term, features_limits, range_min, range_max):

      if term["limit"] == 'fmin':
          limit = range_min

      elif term["limit"] == "fmax":
          limit = range_max

      else:
          limit = features_limits[term['feature']][term['limit']]


      return limit
  @staticmethod
  def write_variable_constraints(file, features_limits, scalers, range_min, range_max):
      """
          Write complex constraints were there
          is a funcition in the feature as norm, lmin, lmax 
      """

#       constraints = Constraints.read_constraints("variable_constraints.json", False)
      constraints = variable_constraints
      constraints_temp = constraints.copy()
    
    
      for k, constraint in constraints.items():
        for sentence in constraint:
          if 'feature' in sentence and sentence['feature'] not in features_limits.keys():
            constraints_temp.pop(k)
            break

      
      constraints = constraints_temp
        

      for constraint, terms in constraints.items():

          parsed_terms = Constraints.parse_complex_constraints(terms, features_limits,
                                                   scalers, range_min, range_max)
          
          solver_operations.write_constraint(file, constraint, parsed_terms)

  
  # plant_module functions: specific functions for this plant
  @staticmethod
  def write_special_constraints(file, scalers, datasets, production_query, fmin, fmax):

#       Constraints.write_power(file, scalers, production_query, datasets)
#       Constraints.write_density(file, scalers)
#       Constraints.write_filter_rotation(file, scalers)
#       Constraints.write_mill_power(file, scalers, datasets, production_query)
#       Constraints.write_mill_body(file, scalers, datasets, production_query)
      Constraints.write_mill_feed_rate(file, scalers)
#      Constraints.write_filter_power_equality(file, scalers)
      Constraints.write_gran_ocs_tm_equality(file, scalers)
#       Constraints.write_wind_box_calc_equality(file, scalers, fmin, fmax)
#       Constraints.write_vent_rotation(file, scalers, fmax)
#       Constraints.write_mean_nive_equality(file, scalers)
      Constraints.write_gran_ocs_tm_min(file, scalers)
#       Constraints.write_ascending_gq(file, scalers)
      Constraints.write_compressao_min_lim(file, scalers)
#       Constraints.write_minimal_temp(file, scalers)
      Constraints.write_calcario_equal(file, scalers)
      Constraints.write_temp_increasing(file, scalers, datasets, fmin, fmax)
      Constraints.write_qtde_filtros(file, scalers, fmax)
#       Constraints.write_vazao_antracito_min_lim(file, scalers)
#       Constraints.write_vazao_antracito(file, scalers)

  @staticmethod
  def write_temp_increasing(file, scalers, datasets, range_min, range_max):
    
    constraint = []

    for i in range(3, 13, 1):
        
        constraint = []

        temp1 = 'TEMP1_I@08QU-QU-855I-GQ{:02}'.format(i)
        temp2 = 'TEMP1_I@08QU-QU-855I-GQ{:02}'.format(i+1)


        constraint_name = '{}_{}_increasing_{}_{}'.format(temp1, temp2, range_min, range_max)

        bm = (scalers[temp1].data_min_[0] - scalers[temp2].data_min_[0]) - 2
        
        constraint.append((temp1, scalers[temp1].data_range_[0]))
        constraint.append((temp2, -scalers[temp2].data_range_[0]))
        constraint.append(('LTE', -bm))
        solver_operations.write_constraint(file, constraint_name, constraint)
        
  @staticmethod
  def write_minimal_temp(file, scalers):

    vent_rotation_token = 'TEMP1_I@08QU-QU-855I-GQ{:02}'.format    
        
#     constraint_sulfix = '_gte_500'
#     constraint = []
#     value = 1
#     token = vent_rotation_token(value)
#     constraint.append((vent_rotation_token(value), scalers[vent_rotation_token(value)].data_range_[0]))
#     constraint.append(("GT", operations.normalize_feature(scalers, token, (500 + increment*15))))
#     solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)

    constraint_sulfix = '_gte_gq3'
    constraint = []
    value = 3
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 830)))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq4'
    constraint = []
    value = 4
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 920)))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq5'
    constraint = []
    value = 5
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 990) ))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq6'
    constraint = []
    value = 6
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 1040) ))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq7'
    constraint = []
    value = 7
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 1090) ))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq8'
    constraint = []
    value = 8
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 1170) ))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq9'
    constraint = []
    value = 9
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 1240) ))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq10'
    constraint = []
    value = 10
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 1250) ))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq11'
    constraint = []
    value = 11
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 1250) ))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq12'
    constraint = []
    value = 12
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 1250) ))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq13'
    constraint = []
    value = 13
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 1270) ))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq14'
    constraint = []
    value = 14
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 1250) ))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq15'
    constraint = []
    value = 15
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 1230) ))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_gq16'
    constraint = []
    value = 16
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), 1))
    constraint.append(("GT", operations.normalize_feature(scalers, token, 1230) ))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
  @staticmethod
  def write_vent_rotation(file, scalers, range_max):

    vent_rotation_token = 'ROTA1_I@08QU-PF-852I-{:02}M1'.format

    increment = ((int(range_max)-700)/50)
        
    constraint_sulfix = '_gte_500'
    constraint = []
    value = 1
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), scalers[vent_rotation_token(value)].data_range_[0]))
    constraint.append(("GT", operations.normalize_feature(scalers, token, (500 + increment*15)) - scalers[vent_rotation_token(value)].data_min_[0]))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)

    constraint_sulfix = '_gte_400'
    constraint = []
    value = 2
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), scalers[vent_rotation_token(value)].data_range_[0]))
    constraint.append(("GT", operations.normalize_feature(scalers, token, (380 + increment*5)) - scalers[vent_rotation_token(value)].data_min_[0]))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_400'
    constraint = []
    value = 3
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), scalers[vent_rotation_token(value)].data_range_[0]))
    constraint.append(("GT", operations.normalize_feature(scalers, token, (380 + increment*5)) - scalers[vent_rotation_token(value)].data_min_[0]))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_320'
    constraint = []
    value = 4
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), scalers[vent_rotation_token(value)].data_range_[0]))
    constraint.append(("GT", operations.normalize_feature(scalers, token, (320 + increment*15))  - scalers[vent_rotation_token(value)].data_min_[0]))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_515'
    constraint = []
    value = 7
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), scalers[vent_rotation_token(value)].data_range_[0]))
    constraint.append(("GT", operations.normalize_feature(scalers, token, (515 + increment*5))  - scalers[vent_rotation_token(value)].data_min_[0]))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint)
    
    constraint_sulfix = '_gte_560'
    constraint = []
    value = 8
    token = vent_rotation_token(value)
    constraint.append((vent_rotation_token(value), scalers[vent_rotation_token(value)].data_range_[0]))
    constraint.append(("GT", operations.normalize_feature(scalers, token, (560 + increment*5))  - scalers[vent_rotation_token(value)].data_min_[0]))
    solver_operations.write_constraint(file, vent_rotation_token(value) + constraint_sulfix, constraint) 
    

    
    
  @staticmethod
  def write_mean_nive_equality(file, scalers):
      constraint_name = 'mean_nive_equality'
      terms = []
      
      bm = sum([scalers['nive'].data_min_[0] for x in range(1,7)])
      bm += -6 * scalers['Media_NIVE1_6_QU-FR-851I-01M1'].data_min_[0]
      
      
      
      terms.append(('Media_NIVE1_6_QU-FR-851I-01M1', 6 * scalers['Media_NIVE1_6_QU-FR-851I-01M1'].data_range_[0]))
      terms.extend([('NIVE{:01}_I@08QU-FR-851I-01M1'.format(x), -scalers['nive'].data_range_[0]) for x in range(2, 7)])
      terms.append(('NIVE1_C@08QU-FR-851I-01M1', -scalers['nive'].data_range_[0]))
      terms.append(('E', bm))
      solver_operations.write_constraint(file, constraint_name, terms)
  
  @staticmethod
  def write_gran_ocs_tm_equality(file, scalers):
              
        # Equalidade das granulometrias como solicitado pelo rodrigo        
        tag_base =  'GRAN_OCS_TM@08PE-BD-840I-10'
        for tag_gran in [x for x in scalers.keys() if 'GRAN_OCS_TM@08PE-BD-840I-' in x and not('10' in x)]:
            constraint_name = tag_base + '_igual_' + tag_gran
            bm = scalers[tag_base].data_min_[0] - scalers[tag_gran].data_min_[0]
            
            terms = [
                (tag_base, -scalers[tag_base].data_range_[0]),
                (tag_gran, scalers[tag_gran].data_range_[0]),
                ('E', bm)
            ]
            
            solver_operations.write_constraint(file, constraint_name, terms)

  @staticmethod
  def write_gran_ocs_tm_min(file, scalers):
              
        # Equalidade das granulometrias como solicitado pelo rodrigo        
        tag_base =  'GRAN_OCS_TM@08PE-BD-840I-10'
        for tag_gran in [x for x in scalers.keys() if 'GRAN_OCS_TM@08PE-BD-840I-' in x]:
            constraint_name = tag_gran + 'more_than13' 
            bm = -1 * scalers[tag_gran].data_min_[0]
            
            terms = [
                (tag_gran, scalers[tag_gran].data_range_[0]),
                ('GTE', 13 + bm)
            ]
            
            solver_operations.write_constraint(file, constraint_name, terms)

            
  @staticmethod
  def write_wind_box_calc_equality(file, scalers, fmin, fmax):
      calcs = {
        'Media_PRES1_I_QU-WB-851I-01_03A': [
                            'PRES1_I@08QU-WB-851I-01', 
                            'PRES1_I@08QU-WB-851I-02',
                            'PRES1_I@08QU-WB-851I-03A'
                    ],
  
  
        'Media_PRES1_I_QU-WB-851I-04_08': [
                            'PRES1_I@08QU-WB-851I-04', 'PRES1_I@08QU-WB-851I-05',
                            'PRES1_I@08QU-WB-851I-06', 'PRES1_I@08QU-WB-851I-07', 
                            'PRES1_I@08QU-WB-851I-08'
                    ],
  
  
        'Media_PRES1_I_QU-WB-851I-09_13': [
                            'PRES1_I@08QU-WB-851I-09', 'PRES1_I@08QU-WB-851I-10', 
                            'PRES1_I@08QU-WB-851I-11', 'PRES1_I@08QU-WB-851I-12', 
                            'PRES1_I@08QU-WB-851I-13'
                    ],
      }
      
      for variable in calcs.keys():
          constraint_name = '{}_calc_equality'.format(variable)
          n_variables = len(calcs[variable])

          bm = -1 * sum([scalers[x].data_min_[0] for x in calcs[variable]])
          bm +=   (n_variables * fmax * scalers[variable].data_min_[0])
          
          terms = [(v, scalers[v].data_range_[0]) for v in calcs[variable]]
          terms.append((variable, -n_variables * fmax * scalers[variable].data_range_[0]))
          
          terms.append(('E', bm))
          
          solver_operations.write_constraint(file, constraint_name, terms)

           
               
          
      
    
  @staticmethod
  def write_power(file, scalers, production_query, datasets):
      df_all = pd.concat(datasets.values(), axis=1)
      max_columns = df_all.loc[production_query, [x for x in df_all.columns if 'POTE' in x and 'BV' in x]].max()
      max_intersection_value = max_columns[max_columns > 0].min()

      token = "POTE1_I@08FI-BV-827I-{:02}M1".format
      func_token = 'FUNC_BV_'

      for value in range(1, 11):          
          if value % 5 == 0:
              func_token = "FUNC1_D@08FI-BV-827I-{:02}RM1".format(value)
              pot_token = "POTE1_I@08FI-BV-827I-{:02}RM1".format(value)          
          else:
              func_token = "FUNC1_D@08FI-BV-827I{:02}M1".format(value)
              pot_token = token(value)
          
          
          
          norm_value = operations.normalize_feature(scalers, pot_token, max_intersection_value) if scalers[pot_token].data_range_[0] > 0 else 1
          
          constraint_name = 'potencia_filtro_bv{}'.format(value)
          terms = []
          terms.append((pot_token, -1))
          terms.append((func_token, norm_value ))
          terms.append(('GT', 300))
                    
          solver_operations.write_constraint(file, constraint_name, terms)


  @staticmethod
  def write_compressao_min_lim(file, scalers):
    terms = []
    
    constraint_name = 'compressao_min'
    terms.append(('COMP_MCOMP_PQ_L@08QU', 1))
    terms.append(('GTE',  operations.normalize_feature(scalers, 'COMP_MCOMP_PQ_L@08QU', 270)))
    
    solver_operations.write_constraint(file, constraint_name, terms) 
    
  @staticmethod
  def write_vazao_antracito_min_lim(file, scalers):
    terms = []
    
    constraint_name = 'vazao_antracito_min'
    terms.append(('Soma_PESO1_I_MO-BW-813I-03_04', 1))
    terms.append(('GT',  operations.normalize_feature(scalers, 'Soma_PESO1_I_MO-BW-813I-03_04', 25)))
    
    solver_operations.write_constraint(file, constraint_name, terms)
    
  @staticmethod
  def write_calcario_equal(file, scalers):
    terms = []
    bm = (scalers['PESO1_I@08MO-BW-821I-01M1'].data_range_[0] + scalers['PESO1_I@08MO-BW-821I-02M1'].data_range_[0] + scalers['PESO1_I@08MO-BW-821I-03M1'].data_range_[0])   
    bm = bm * 0.8762 * scalers['calcario'].data_range_[0]
    bm = operations.normalize_feature(scalers, 'PESO1_I@08MO-BW-813I-01M1', bm)
    
    constraint_name = 'calcario_equality'
    terms.append(('calcario', -bm))
    terms.append(('PESO1_I@08MO-BW-813I-01M1', 1))
    terms.append(('GTE',  0))
    
    solver_operations.write_constraint(file, constraint_name, terms)

  @staticmethod
  def write_vazao_antracito(file, scalers):
    terms = []
    
    constraint_name = 'vazao_antracito_equality'
    terms.append(('vazao_antracito', -scalers['vazao_antracito'].data_range_[0]))
    terms.append(('PESO1_I@08MO-BW-813I-03M1', 1))
    terms.append(('PESO1_I@08MO-BW-813I-04M1', 1))
    terms.append(('E',  0))
    
    solver_operations.write_constraint(file, constraint_name, terms)
    
  @staticmethod
  def write_filter_power_equality(file, scalers):
    terms = []
    
    constraint_name = 'energia_filtro_equals_CEF'
    terms.append(('Calculo da Energia da Filtragem', scalers['Calculo da Energia da Filtragem'].data_range_[0]))
    terms.append(('energia_filtro', -1))
    terms.append(('E', -scalers['Calculo da Energia da Filtragem'].data_min_[0]))
    
    solver_operations.write_constraint(file, constraint_name, terms)
    
  @staticmethod
  def write_qtde_filtros(file, scalers, fmax):
     
      constraint_name = 'taxa_filtros_vs_producao'
      terms = []
      for i in range(1,11):
          v_func = 'FUNC1_D@08FI-FL-827I-{:02}'.format(i)
          
          if i % 5 == 0:
              v_func = "FUNC1_D@08FI-FL-827I-{:02}R".format(i)

          terms.append((v_func, 150))

      terms.append(('GTE', fmax))
      solver_operations.write_constraint(file, constraint_name, terms)
      
  @staticmethod
  def write_mill_feed_rate(file, scalers):
      # Taxa de alimentacao moinho
      constraint_name = 'taxa_alimentacao_moinho_vs_producao'
      aproveitamento_massa_moinho = 0.8 #0.8762
      terms = []
      for i in range(1,4):
          v_func = 'FUNC1_D@08MO-BW-821I-{:02}M1'.format(i) # v_func = 'FUNC1_D@08MO-MO-821I-{:02}M1'.format(i)
          v_taxa = 'PESO1_I@08MO-BW-821I-{:02}M1'.format(i) # PESO1_I@08MO-BW-821I-{:02}M1
          coef = aproveitamento_massa_moinho * scalers[v_taxa].data_range_[0]

          terms.append((v_func, -aproveitamento_massa_moinho * scalers[v_taxa].data_min_[0]))
          terms.append((v_taxa, coef))

      terms.append(('PROD_PQ_Y@08US', -scalers['PROD_PQ_Y@08US'].data_range_[0]))
      terms.append(('E', scalers['PROD_PQ_Y@08US'].data_min_[0]))
      solver_operations.write_constraint(file, constraint_name, terms)

  @staticmethod
  def write_density(file, scalers):

      token = "DENS1_C@08HO-BP-826I-{:02}".format
      func_token = 'FUNC_DENS'

      index = 1
      for value in range(5, 9):

          if value % 2 == 0:
              dens_token = "DENS1_C@08HO-BP-826I-{:02}R".format(value)
          else:
              dens_token = token(value)

          second_coef = -operations.normalize_feature(scalers, dens_token, 1.95)
          constraint_name = "densidade_min_{}".format(index)

          func_param = func_token + str(index)
          Constraints.generic_term_writing(file, dens_token, func_param, 1, second_coef,
                               "GTE", 0, False, constraint_name)

          func_param = func_token + str(index)
          second_coef = -operations.normalize_feature(scalers, dens_token, 2.2)
          constraint_name = "densidade_max_{}".format(index)
          Constraints.generic_term_writing(file, dens_token, func_param, 1, second_coef,
                               "LTE", 0, False, constraint_name)

          index += 1

  @staticmethod
  def write_filter_rotation(file, scalers):

      first_token = "FUNC1_D@08FI-FL-827I-{:02}"

      for value in range(1, 11):

          terms = []

          if value % 5 == 0:
              func_token = "FUNC1_D@08FI-FL-827I-{:02}R".format(value)

          else:
              func_token = first_token.format(value)

          second_coef = -operations.normalize_feature(scalers, rota_token, 0.8)
          constraint_name = "rotacao_filtro_min_{}".format(value)
          Constraints.generic_term_writing(file, rota_token, func_token, 1, second_coef,
                               "GTE", 0, False, constraint_name)


          second_coef = -operations.normalize_feature(scalers, rota_token, 0.8)
          constraint_name = "rotacao_filtro_max_{}".format(value)
          Constraints.generic_term_writing(file, rota_token, func_token, 1, second_coef,
                               "LTE", 0, False, constraint_name)

  @staticmethod
  def write_mill_power(file, scalers, datasets, production_query):

      #"energia_moinho_min_"
      token = "Consumo de Energia (base minério úmido) kWh/ton {}"

      power_token = 'energia_moinho{}'.format
      func_token = 'FUNC1_D@08MO-MO-821I-{:02}M1'.format

      for value in range(1, 4):

          terms = []
          feature = token.format(value)
          df = Limits.define_work_dataset(feature, datasets, production_query)

          lmin = df.rolling(12, min_periods = 1).mean().quantile(0.05)
          lmax = df.rolling(12, min_periods = 1).mean().quantile(0.99)


          if np.isnan(lmin) or np.isnan(lmax):
              raise(Exception('os limites da energia dos moinhos não podem ser NAN'))

          first_token = "energia_moinho_min{}".format(value)
          Constraints.generic_term_writing(file, power_token(value), func_token(value), 1, -lmin, "GTE", 0, False, first_token)

          first_token = "energia_moinho_max{}".format(value)
          Constraints.generic_term_writing(file, power_token(value), func_token(value), 1, -lmax, "LTE", 0, False, first_token)

  
  @staticmethod
  def write_mill_body(file, scalers, datasets, production_query):


      grinder_body_token = 'corpo_moedor_especifico_{}'.format
      func_token = 'FUNC1_D@08MO-MO-821I-{:02}M1'.format

      for value in range(1, 4):

          terms = []
          df = Limits.define_work_dataset(grinder_body_token(value), datasets, production_query)
          lmin, lmax = Limits.limits_by_rolling_mean(df, scalers,
                                                     grinder_body_token(value), 24, 0.25, 0.75)

          if np.isnan(lmin):
              lmin = 0

          if np.isnan(lmax):
              lmax = 1

          commom_token = "corpo_m_moinho_min{}".format(value)
          func_param =  func_token(value)
          Constraints.generic_term_writing(file, grinder_body_token(value), func_param, 1, -lmin, "GTE", 0, False, commom_token)

          commom_token = "corpo_m_moinho_max{}".format(value)
          Constraints.generic_term_writing(file, grinder_body_token(value), func_param, 1, -lmax, "LTE", 0, False, commom_token)

  @staticmethod
  def write_ascending_gq(file, scalers):
       
      for gq_number in range(3, 11):
          
          first_token = 'TEMP1_I@08QU-QU-855I-GQ{:02}'.format(gq_number - 1)
          second_token = 'TEMP1_I@08QU-QU-855I-GQ{:02}'.format(gq_number)
          
          if first_token not in scalers:
              first_token = 'TEMP1_I@08QU-QU-855I-GQ{:02}'.format(gq_number - 2)
          
          constraint_name = '{}_greater_than_{}'.format(first_token, second_token)
          bm = scalers[second_token].data_min_[0] - scalers[first_token].data_min_[0]
          
          terms = []
          terms.append((first_token, scalers[first_token].data_range_[0]))
          terms.append((second_token, - scalers[second_token].data_range_[0]))
          terms.append(('LT', bm))
          
          solver_operations.write_constraint(file, constraint_name, terms)

  @staticmethod
  def generic_term_writing(file, first_token, second_token,
                           first_coef, second_coef, operator,
                           final_coef, commom_token=True, constraint_name=None):

      terms = []
      if commom_token:
          terms.append((second_token, first_coef))
          terms.append((second_token, second_coef))
          terms.append((operator, final_coef))
          solver_operations.write_constraint(file, first_token, terms)

      else:
          terms.append((first_token, first_coef))
          terms.append((second_token, second_coef))
          terms.append((operator, final_coef))
          solver_operations.write_constraint(file, constraint_name, terms)

  
  @staticmethod
  def measure_new_coef(term, scalers, norm_feature=None):

      feature = term["feature"]

      new_coef = 1

      if "operator" in term.keys():

          if term["operator"] == "norm":

              if norm_feature:
                  feature = norm_feature

              new_coef = operations.normalize_feature(scalers, feature, term["limit"])

          elif term["operator"] == "scaler":

              if term["position"] == "range":

                  new_coef = scalers[feature].data_range_[0]

              elif term["position"] == "min":

                  new_coef = scalers[feature].data_min_[0]

              else: # it is max
                  new_coef = scalers[feature].data_max_[0]

      new_coef = term['coef'] * new_coef


      return term, new_coef

  @staticmethod  
  def define_operator_term(terms):

      operations = ["LT", "LTE", "GTE", "GT", "E"]

      for index, term in enumerate(terms):
          condition_one = any (operation in term.values() for operation in operations)
          if condition_one and "operation" not in term.keys():
              operator = (term["operator"], term['coef'])
              break

      terms.pop(index)

      return terms, operator

  @staticmethod
  def parse_range_complex_constraints(file, scalers):

#       constraints = Constraints.read_constraints("range_complex_constraints.json", False)
      constraints = range_complex_constraints
      constraints = Constraints.parse_data(constraints)

      for constraint_name, terms in constraints.items():

          # defining the constraint name
          start, end = terms[0]["start"], terms[0]["end"] + 1
          constraint_name = constraint_name.format
          constraints_names = [constraint_name(value)
                               for value in range(start, end)]

          terms, operator = Constraints.define_operator_term(terms)

          range_terms = list(map(lambda term: Constraints.define_range_terms(term, scalers), terms))


          composed_terms = []
          for term_index in range(0, len(range_terms[0])):
              composed_terms.append(list(map(lambda term: term[term_index], range_terms)))


          for constraint_name, terms in zip(constraints_names, composed_terms):
              terms = list(terms)
              terms.append(operator)
              solver_operations.write_constraint(file, constraint_name, tuple(terms))

  @staticmethod
  def write_complex_constraints(file, scalers):


#       constraints = Constraints.read_constraints("complex_constraints.json", False)
      constraints = complex_constraints

      
      for constraint_name, terms in constraints.items():
        
          new_terms = Constraints.parse_type_complex_terms(constraint_name,
                                               terms, scalers)
    
          solver_operations.write_constraint(file, constraint_name, new_terms)


  @staticmethod
  def parse_type_complex_terms(constraint_name, terms, scalers):

      operations = ["LT", "LTE", "GTE", "GT", "E"]

      new_terms, operator = [], []


      for term in terms:

          if "start" in term.keys() and "end" in term.keys():

              new_terms.extend(Constraints.define_range_terms(term, scalers))

          elif any (operation in term.values() for operation in operations):

              operator = term.copy()

          elif "terms" in term.keys(): # complex factor after the operator

              right_terms = Constraints.parse_type_complex_terms(constraint_name, term["terms"], scalers)


          elif "operator" in term.keys() and "position" in term.keys():

              _, new_coef = Constraints.measure_new_coef(term, scalers)

              new_terms.append((term['feature'], new_coef))

          else: # it is a static feature

              new_terms.append((term["feature"], term['coef']))


      if isinstance(operator, dict):

          sum_coefs = sum([list(value)[1] for value in right_terms])

          operator = (operator['operator'], operator['coef'] * sum_coefs)

          new_terms.append(operator)


      return new_terms