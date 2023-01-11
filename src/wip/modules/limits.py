""" Limits for the solver variables """
import importlib

class Limits:
    
  def __init__(self):
      pass

  @staticmethod
  def limits_by_minmax(limits, feature, query_range):

      values = limits[feature][query_range]

      return values.min(), values.max()

  @staticmethod
  def limits_by_norm(feature, norm_value_one, norm_value_two=None):

      limit_one = normalize_var(feature, norm_value_one)
      if norm_value_two:
         return limit_one, normalize_var(feature, norm_value_two)

      return limit_one

  @staticmethod
  def limits_by_quantile(limits, feature, query_range,
                         quant_value_one, quant_value_two=None):

      limit = limits[feature][query_range]
      if quant_value_two:
          return limit.quantile(quant_value_one), limit.quantile(quant_value_two)

      return limit.quantile(quant_value_one)

  @staticmethod
  def limits_by_rolling_mean(df, scalers, feature, rolling, quant_one, quant_two):

      roll_mean = df.rolling(rolling).mean().quantile

      min_limit = roll_mean(quant_one) - scalers[feature].data_min_[0]
      max_limit = roll_mean(quant_two) - scalers[feature].data_min_[0]

      range_value = scalers[feature].data_range_[0]


      return min_limit/range_value, max_limit/range_value

  @staticmethod
  def parse_limits(limits):
      """
          Parse json limits:
              # tag limits has tags with the same limits as others
      """

      for tag, value in limits.items():
          if isinstance(value, str):
              limits[tag] = limits[value]

      return limits

  @staticmethod
  def read_limits(limits, feature=None):
      """
          Read and return the limits values
      """
#       base_path = 'abfss://insight@usazu1valesa001.dfs.core.windows.net/Workarea/Pelletizing/Process_Optimization/Usina08/Otimizacao/files/'
#       try:
#         limits = load_adls_json(base_path, file)
#       except:
#         raise Excepction("Impossível realizar a leitura do arquivo {} no path {}".format(file, base_path))
        
      limits = Limits.parse_limits(limits)

      # In this case, the operation applied over that tag is different
      if feature and not feature in limits.keys():
          return None

      return limits

  @staticmethod
  def define_limit_by_quantile(feature, models_features, production_query, limits):
      """
          Args:
              Feature name and the array with the feature values
      """

#       limits = Limits.read_limits("quantile_limit.json", feature)

      if not limits:
          return None, None

      feature_quantile = models_features[feature][production_query].quantile

      if "min" in limits[feature].keys() and "max" in limits[feature].keys():

          min_value, max_value = limits[feature]['min'], limits[feature]['max']

          return feature_quantile(min_value), feature_quantile(max_value)

      elif "min" in limits[feature].keys():
          return feature_quantile(limits[feature]['min']), None

      else:
          return None, feature_quantile(limits[feature]['max'])

  @staticmethod
  def define_limit_by_normalization(scalers, feature, limits):
      """ 
          Define limits min and max by var normalization

          Returns both value when lmin and lmax are defined
          Return a left vlaue when only lmin is defined
          Return a right value when only lmax is defined
      """

      arguments = {'scalers': scalers, 'feature': feature}
      if type(limits[feature]) == 'str':
          limits[feature] = limits[limits[feature]]
          
      if len(limits[feature]) == 2:
          arg_min = {**arguments, 'norm_value': limits[feature]['min']}
          arg_max = {**arguments, 'norm_value': limits[feature]['max']}

          return operations.normalize_feature(**arg_min), operations.normalize_feature(**arg_max)

      # in this case has only one of the values
      # getting the min or max value in the limits
      try:  
        if 'min' in limits[feature].keys():
            args = {**arguments, 'norm_value': limits[feature]['min']}
            return operations.normalize_feature(**args), None
      except:
        raise Exception(feature)
      args = {**arguments, 'norm_value': limits[feature]['max']}
      return None, operations.normalize_feature(**args)

  @staticmethod
  def define_work_dataset(feature, datasets, production_query):

      dataset = operations.retrieve_dataset(feature, datasets)[0]

      production_query = production_query.rename("production")
      df = datasets[dataset].join(production_query)

      df = df[df['production']==True]

      df = df[df['status'] == 1][feature]

      return df

  @staticmethod
  def define_limit_by_rolling_mean(feature, production_query, datasets, scalers, rolling_limits):

      df = Limits.define_work_dataset(feature, datasets, production_query)

      return Limits.limits_by_rolling_mean(df, scalers, feature,
                                    rolling_limits[feature]['rolling'],
                                    rolling_limits[feature]['quant_one'],
                                    rolling_limits[feature]['quant_two'])

  @staticmethod
  def define_bentonita_limit(feature, datasets, production_query, scalers):

      df = Limits.define_work_dataset(feature, datasets, production_query)
      lmin, _ = Limits.limits_by_rolling_mean(df, scalers, feature, 24, 0.25, 0.25)
      aux = df.rolling(24).mean().quantile(0.25) + df.rolling(24).mean().quantile(0.25) * 0.05

      if aux > 0.0053:
          lmax = operations.normalize_feature(scalers, feature, aux)
      else:
          lmax = operations.normalize_feature(scalers, feature, 0.0053)

      return lmin, lmax
    
  @staticmethod
  def define_constant_limits(feature, limits):

      for  key in limits.keys():
          if feature.startswith(key):
              feature = key
              break

      return limits[feature]['min'], limits[feature]['max']