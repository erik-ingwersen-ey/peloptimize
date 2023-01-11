""" Solver generic methods """

import glob
import os
import re
import subprocess
import math

import numpy as np
import pandas as pd

import importlib

class solver_operations:

    @staticmethod
    def write_constraint(file, constraint, terms, target=False, description=False):
        """ 
            Write a set of constraints in a file, most specify:
            file: file name
            constraint: name of the restriction that will be added 
            terms: terms that compose the constraint equation
            target: features that are target are treated in
            write_descriptive_contraints method
        """
        operators = ['E', 'LTE', 'LT', 'GT', 'GTE']

        if description:


            target, coef, description = terms
            print("{}; {}; {}; {}" .format(constraint, target, coef, description), file=file)


        elif target:
            target, new_tag, new_coef = terms

            print("{}; {}; {}; {}" .format(target, new_tag,
                                           new_coef, constraint), file=file)
        else:

            if constraint:

                for index, term in enumerate(terms):

                    verify_int = isinstance(term, int)
                    verify_float = isinstance(term, float)

                    # it is not the last
                    verify_last = index != len(terms) - 1

                    if not isinstance(term, tuple) and math.fabs(float(term)) <= 0.00001:
                        continue


                    if (verify_int or verify_float) and verify_last:
                        print("{}; {:.5f}" .format(constraint,
                              term), file=file)
                    else:
                        # term is a tuple that can be composed by:
                        # a feature and a coeficient value or,
                        # a operation and a coeficient value

                        operation, value = term
                        if operation in operators:
                            print("{}; {} {}" .format(constraint,
                                    operation, value), file=file)
                        else:
                            print("{}; {}; {}" .format(constraint,
                                    operation, value), file=file)

            else:

                feature, value = terms
                if math.fabs(float(value)) > 0.00001:
                    print("{}; {}" .format(feature, value), file=file)


    @staticmethod
    def adjust_real_cost(real_cost, features, mult_coef=1, div_coef=3):
        """Adjust the real cost of some features"""

        for key in real_cost.keys():
            if key in features:
                real_cost[key] = (real_cost[key] * mult_coef)/div_coef

        return real_cost


    @staticmethod
    def retrieve_best_model(model, models_results, metric='mape'):
        """ Sort the models by it metrics """

        return sorted(models_results[model],
                      key= lambda x: x['metrics'][metric])[0]


    @staticmethod
    def retrieve_model_coeficients(model, models_results):
        """
            Returns for each feature in a model it linear coeficients,
            for the best model
        """
        best_model = solver_operations.retrieve_best_model(model, models_results)


        # coeficient of each feeature used in the model
        return zip(best_model['columns'], best_model['model'].coef_)

    @staticmethod
    def write_descriptive_contraints(file, model_target, datasets, df_detailed,
                                     scalers, models_coeficients, features_coeficient,
                                     models_results):
        """ 

            Model target -> name of the selected model


            Some target constraints has a different a different treatment
            when compared to other features.

            This method treats the features 
        """
        unnormalize_constant = 0
        #print(list(features_coeficient))
        for tag, coef in features_coeficient:
    #             log.debug(f'{tag}')
            # if that tag repeats in the dataframe
            tag_count = df_detailed.columns.tolist().count(tag)
            if    tag_count > 1:
                description = df_detailed[tag].loc["Descrição"].iloc[0]
            elif tag_count == 1:
                description = df_detailed[tag]['Descrição']
            else:
                description = ''

            if tag in constants.define_targets(datasets):

                new_coef, constant = operations.unnormalize_feature(scalers, tag, coef)
                new_tag = constants.TARGETS_IN_MODEL[tag]

    #                 log.debug(f'DEBUG {type(models_coeficients)}, {models_coeficients}')
    #                 log.debug(f'DEBUG: {model_target, tag}')
                unnormalize_constant -= constant
                models_coeficients[model_target][tag] = new_coef

                solver_operations.write_constraint(file, model_target, (new_tag, new_coef, description), description=True)

            else:

                solver_operations.write_constraint(file, model_target, (tag, coef, description), description=True)

        solver_operations.write_simple_constraints(file, model_target, models_results, unnormalize_constant)

        return models_coeficients

    @staticmethod
    def write_simple_constraints(file, model_target, models_results, unnormalize_constant):

        # Will return None when the target is rota_disco,
        # and when the tag limit is defined as None
        limit = constants.LIMITS[model_target] if not model_target.startswith('rota_disco_') else None

        best_conf = solver_operations.retrieve_best_model(model_target, models_results)

        if not limit:


            # o problema é aqui
            if 'custo' in model_target:
                solver_operations.write_constraint(file, model_target, [(model_target.replace("custo_", ""), -1)])

            else:
                solver_operations.write_constraint(file, model_target, [(model_target, -1)])

            feat_target = None

    #             if model_target.startswith('energia_moinho'):
    #                 feat_target = 'FUNC1_D@08MO-MO-821I-{:02}M1'.format(int(model_target[-1]))

            if model_target.startswith('rota_disco'):
                try:
                    #encontra o numero do disco para a associação entre a tag de funcionamento.
                    disc_number = re.findall(r'\d+', model_target)[0]
                except:
                    raise Exception('no disk number provided')

                feat_target = 'FUNC1_D@08PE-BD-840I-{:02}M1'.format(int(disc_number))

            if feat_target:
                solver_operations.write_constraint(file, model_target,
                                [(feat_target, best_conf['model'].intercept_ + unnormalize_constant)])

            else:
                solver_operations.write_constraint(file, None,
                                (model_target, best_conf['model'].intercept_ + unnormalize_constant))

            solver_operations.write_constraint(file, model_target, [('E', '0')])

        else:


            operator, value = constants.LIMITS[model_target]

            solver_operations.write_constraint(file, None,
                            (model_target, best_conf['model'].intercept_ + unnormalize_constant))

            solver_operations.write_constraint(file, model_target, [(operator, value)])

    @staticmethod
    def define_range_constraints(token, range_start, range_end, step=1):
        """
            function will specify the values that will be applied over the data
            it can be defined by a dict, list or even a function 

        """

        # considering that in the token will be a
        # acting range, that has to be defined
        token = token.format
        return [token(i) for i in range(range_start, range_end, step)]

    @staticmethod
    def scale_optimization_tags(scalers, real_cost):

        opt_keys = list(filter(lambda col: col not in constants.TARGETS_IN_MODEL.values() and col in scalers.keys(),
                               real_cost.keys()))

        data_range = 0
        for key in opt_keys:
            data_range = scalers[key].data_range_[0] if scalers[key].data_range_[0] > 0 else data_range
            real_cost[key] = data_range * real_cost[key]


        return real_cost

    @staticmethod
    def unnormalize_optimization_tags(scalers, real_cost):

      opt_keys = list(filter(lambda col: col not in constants.TARGETS_IN_MODEL.values() and col in scalers.keys(),
                             real_cost.keys()))


      for key in opt_keys:
          data_range = scalers[key].data_range_[0] if scalers[key].data_range_[0] > 0 else data_range
          real_cost[key] = real_cost[key] / data_range


      return real_cost


    @staticmethod
    def parse_variables_name(variables_name):

        return list(map(lambda var: re.search(r'\((.*)\)', var).group(1), variables_name))

    @staticmethod
    def parse_coeficients(coeficients):

        return list(map(lambda coef: float(coef.replace(',', '.')), coeficients))

#     @staticmethod
#     def measure_final_cost(scaled_info, real_costs, obj_coef):

#         total_cost = 0


#         for feature in scaled_info.keys():

#             if feature == 'nive':
#                 continue

#             feature_coef = scaled_info[feature][" Value"]
#             objective_coef = obj_coef.loc[obj_coef[' VariableName'] == feature,' ObjCoeff'].values[0]

#             total_cost += feature_coef * (real_costs[feature] if feature in real_costs.keys() else objective_coef)

#         return total_cost

    @staticmethod
    def features_in_scalers(scalers, features):

        scale_features = list(filter(lambda feature: feature in scalers,
                                     features))

        return scale_features


#     @staticmethod
#     def features_to_scale(optmin_df, scalers):

#         scale_features = set(solver_operations.features_in_scalers(scalers, optmin_df[' VariableName']))
#         scaled_info = {feature:    {} for feature in optmin_df[' VariableName']}

#         for feature in scaled_info.keys():
#             scaled_info[feature] = optmin_df.loc[optmin_df[' VariableName'] == feature].to_dict(orient='list')
#             scaled_info[feature] = {key: value[0] for key, value in scaled_info[feature].items()}

#         scaled_info['nive'] = {' Value': 0, " ObjCoeff": 0, " LB": 0, " UB": 0}

#         columns_to_drop = set(['LpIndex', ' VariableType']).intersection(optmin_df.columns)
#         if len(columns_to_drop) > 1:
#           optmin_df.drop(columns_to_drop, axis=1, inplace=True)

#         for feature in scale_features: # features that will scaled
#             # feature_coef # obj_coef # upper_bound # lower_bound

#             for key in scaled_info[feature].keys():

#                 if key.strip() in ['Value', "LB", "UB"]:

#                     norm_value = scaled_info[feature][key] # value that will be normalized


#                     result =    operations.unnormalize_feature(scalers, feature, norm_value, operation='one feature')

#                     if re.match(r'NIVE[1-6]_[I|C]@08QU-FR-851I-01M1', feature):
#                         scaled_info['nive'][key] = result
#                     else:
#                         scaled_info[feature][key] = result

#             scaled_info[feature]["Normed Value"] = norm_value


#         return scaled_info


    @staticmethod
    def save_new_format(df):

        path = './us8/resultado_otimizador.csv'

        df = df.pivot("TAG", 'faixa', 'valor real')
        df.to_csv(path, sep=';', encoding='iso-8859-1')

        operations.replace_string_from_file(path)

    @staticmethod
    def save_solver_results(solver_path, df):

        format_tags = ['minimo', 'maximo', 'valor normalizado', 'valor real']

        for tag in format_tags:
            df[tag] = df[tag].map('{:.5f}'.format)

        columns_order = ['faixa', 'TAG', 'minimo', 'maximo',
                         'valor normalizado', 'valor real', 'custo']

        df = df.reindex(columns=columns_order)

        remove_tags = ['FUNC1_D@08MI-AM-832I-01M1']

        df = df.loc[~df['TAG'].isin(remove_tags)]

        path = f"{solver_path}/resultado_otimizador-formato-antigo.csv"

        df.to_csv(path, sep=';', index=False, encoding='iso-8859-1')

        operations.replace_string_from_file(path)

    #         solver_operations.save_new_format(df)