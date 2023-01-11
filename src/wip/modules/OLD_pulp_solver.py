import pandas as pd
import numpy as np
import glob
import pulp
import os
import re
from multiprocessing import Process
import logging
import sys
# from outputs import *
import json
import unicodedata
# import xlsxwriter

# logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
#                     level=logging.INFO, datefmt='%I:%M:%S', stream=sys.stdout)

def save_test(execute):
    def inner(*args):
        validate = execute(*args)
        df_debug = args[0].getDfDebug()
        df_debug[validate.name] = validate
        return df_debug
    return inner


class AbstractStep(object):


    def __init__(self, df_debug, **args):
        self.__test_name = type(self).__name__
        self.__args = args
        self.__df_debug = df_debug


    def execute(self, **args):
        raise NotImplementedError('Not implemented Method Error')

    def getArgs(self):
        return self.__args

    def getTestName(self):
        return self.__test_name

    def getDfDebug(self):
        return self.__df_debug


class StepsValidator(object):

    def __init__(self, df_debug, raw_columns):
        self.__df_debug = df_debug
        self.__steps = AbstractStep.__subclasses__()
        self.__raw_columns = raw_columns
        
        pulp.LpSolverDefault.msg = 1

    def apply_steps(self):
        for step in self.__steps:
            self.__df_debug = step(self.__df_debug).execute()


    def exportValidation(self):
        '''
        Exporta o Dataframe contendo os testes aplicados aos resultados.
        '''

        df_debug = self.__df_debug

        condition = df_debug.loc[:, ~df_debug.columns.isin(self.__raw_columns.tolist())].sum(1) == 1
        ixs = df_debug.loc[condition, :].index

        writer = pd.ExcelWriter('resultado_otimizador/debug.xlsx', engine='xlsxwriter')
        self.__df_debug.loc[:, self.__raw_columns.tolist()].to_excel(writer, sheet_name='debug')
        workbook = writer.book

        #estilo da cor que marca as linhas que respeitam os testes
        data_format1 = workbook.add_format({'bg_color': '#ff0033'})

        worksheet = workbook.get_worksheet_by_name('debug')
        for row in ixs:
            worksheet.set_row(row, cell_format=data_format1)

        writer.save()


    def getSteps(self):
        return self.__steps

    def getDfDebug(self):
        return self.__df_debug


class RangeValidation(AbstractStep):

    @save_test
    def execute(self):
        df_debug = self.getDfDebug()
        validate = ~((df_debug['minimo'] <= df_debug['valor real']) | (df_debug['maximo'] >= df_debug['valor real']))
        validate = pd.Series(validate, name=self.getTestName())
        return validate

class PulpSolver:

    def __init__(self, path_to_constraints, path_to_costs, constants, solver_option=None):

        self.__variables = {}
        self.probs = {}
        self.df_results = {}
        self.solver_option = solver_option
        self.path_to_constraints = path_to_constraints
        self.path_to_costs = path_to_costs
        self.constants = constants
        self.constraints = {}

        self.solvers = {
            'cplex': pulp.CPLEX(),
            'gurobi': pulp.GUROBI(),
            'glpk': pulp.GLPK(),
            'cbc':  None,
        }

        path_to_objective_function = os.path.join(
                path_to_constraints, 'costs.csv')

        df_obj_function = pd.read_csv(path_to_objective_function, sep=';')
        df_obj_function['Custo'] = df_obj_function['Custo'].apply(
            lambda x: float(x.replace(',', '.')))


        with open(path_to_costs, 'r') as fp:
            self.__costs = json.load(fp)


        self.df_obj_function = df_obj_function

        for file_path in glob.glob(os.path.join(path_to_constraints, 'restricoes-faixa-*-*.txt')):
            self.filepath = file_path
            key = self.extract_key_name(file_path)

            self.parser_restriction_files(file_path, key)

            self.parse_objective_file(key)

            logging.info('done ! \n\n')

    def parser_restriction_files(self, file_path, key):

        with open(file_path, 'r') as f:
            sentences = f.read().splitlines()

        self.probs[key] = pulp.LpProblem(key, pulp.LpMinimize)
        self.__variables[key] = {}
        constraint_old_name = sentences[0].split(';')[0]
        constraint = pulp.LpConstraint()

        logging.info('creating constraints for {}'.format(key))

        for i in range(1, len(sentences)):
            #             sentences[i] = self.sub_specfic_caracters(sentences[i])
            terms = sentences[i].split(';')

            constraint_name = terms.pop(0)

            if constraint_name != constraint_old_name:

                if constraint_old_name in self.probs[key].constraints :
                    constraint.setName(self.sub_specfic_caracters(
                        constraint_old_name + '_v2'))
                else:
                    constraint.setName(
                        self.sub_specfic_caracters(constraint_old_name))

                if constraint.getName() not in self.probs[key].constraints and 'limit_' not in constraint.getName():
                    self.probs[key] += constraint
                    self.constraints[constraint.getName()] = constraint
                    
                
                elif('limit_' not in constraint.getName()):
                    logging.warn("Constraint {} is already in lp problem, skipping".format(constraint.getName()))
                
                constraint = pulp.LpConstraint()
                constraint_old_name = constraint_name

            if (len(terms) == 3) or (len(terms) == 2):
                feature, coef = terms[0], terms[1]
                feature = feature.lstrip()

                constraint = self.mount_multiply(
                    feature, coef, constraint, key)
                
                if '_limit_' not in constraint_name and '_limit_' not in constraint_old_name and ('min' in constraint_name or 'max' in constraint_name):
                    
                    if 'FUNC' not in feature:
                        feature_to_set_limit = feature
                    else:
#                         bound_name = 'max' if 'max' in constraint_name else 'min'
                        bound_value = float(coef.replace(',', '.'))
                        if 'prod' in  constraint_name:
                          print(constraint_name)
                          print(bound_value, feature)
                          
                        
                        self.save_variable_limits(bound_value, constraint_name, key, feature_to_set_limit)
                
                        
              
            elif len(terms) == 1:
                coef = terms[0].split()

                if len(coef) == 1:
                    constraint, value = self.mount_add(coef, constraint)
                    if '_limit_' in constraint_name:
                        self.save_variable_limits(
                            value, constraint_name, key, feature)

                else:
                    constraint, value = self.mount_comparation(
                        key, coef, constraint)

    def parse_objective_file(self, key):

        #         df_obj_function['TAG'] = df_obj_function['TAG'].apply(
        #             lambda x: self.sub_specfic_caracters(x))

        msg = 'Creating objective function using file located in {} for {}'.format(
            self.filepath, key)
        logging.info(msg)

        for k, v in self.probs.items():
            variables_not_created = self.df_obj_function.loc[~self.df_obj_function['TAG'].isin(
                self.__variables[key].keys())]
            variables_not_created['TAG'].apply(
                lambda x: self.add_variable(x, key))

            obj = pulp.lpSum([self.__variables[key][name]['obj'] * cost for name, cost in zip(
                self.df_obj_function['TAG'], self.df_obj_function['Custo'])]), 'objective_function'

            self.probs[key] += obj


    def add_variable(self, feature, key):

        self.__variables[key][feature] = {}


        simple_feature_name = self.sub_specfic_caracters(feature)
        if feature.startswith('FUNC') or feature.startswith('status'):
            self.__variables[key][feature]['obj'] = pulp.LpVariable(
                simple_feature_name, cat="Binary", lowBound=0, upBound=1)
            
            self.__variables[key][feature]['min'] = 0
            self.__variables[key][feature]['max'] = 1

            

        elif feature.startswith('SOMA FUNC') or feature.startswith('qtde') or feature.startswith('SOMA_FUNC'):
            self.__variables[key][feature]['obj'] = pulp.LpVariable(
                simple_feature_name, cat="Integer")

        else:
            self.__variables[key][feature]['obj'] = pulp.LpVariable(
                simple_feature_name)
        

    def mount_multiply(self, feature, coef, constraint, key):

        try:
            coef = float(coef.replace(',', '.'))
        except:
            raise Exception('erro de conversão de tipo valor: {} feature: {} e nome da restrição {}'.format(coef, feature, constraint))

        if feature not in self.__variables[key]:
            self.add_variable(feature, key)

        constraint += self.__variables[key][feature]['obj'] * coef

        return constraint

    def mount_add(self, coef, constraint):

        value = float(coef[0].replace(',', '.'))

        constraint = constraint + value

        return constraint, value

    def mount_comparation(self, key, coef, constraint):

        logical_operator = coef[0]
        value = float(coef[1].replace(',', '.'))

        if logical_operator == 'GTE':
            constraint = constraint >= value

        elif logical_operator == 'LTE':
            constraint = constraint <= value

        elif logical_operator == 'E':
            constraint = constraint == value

        elif logical_operator == 'LT':
            constraint = constraint <= value

        elif logical_operator == 'GT':
            constraint = constraint >= value

        return constraint, value


    def save_variable_limits(self, value, constraint_name, key, feature):          
        limit_key = 'min' if 'min' in constraint_name else 'max'
        
        if limit_key == 'min':
            self.__variables[key][feature]['obj'].lowBound = -1 * value
        else:
            self.__variables[key][feature]['obj'].upBound = -1 * value

        self.__variables[key][feature][limit_key] = -1 * value
        
        self.check_variable_bounds(key, feature)
    
    def check_variable_bounds(self, key, feature):
        pulp_variable = self.__variables[key][feature]['obj']

        if 'min' not in self.__variables[key][feature] or pulp_variable.lowBound == None or np.isnan(pulp_variable.lowBound):
            self.__variables[key][feature]['min'] = 0
            self.__variables[key][feature]['obj'].lowBound = 0
            
#             logging.warning('Variable {} in production range {} without a lb or ub definition. Setting default values'.format(feature, key))
            
        if 'max' not in self.__variables[key][feature] or pulp_variable.upBound == None or np.isnan(pulp_variable.upBound):
            self.__variables[key][feature]['max'] = 1
            self.__variables[key][feature]['obj'].upBound = 1
            
            
          
    def solve_range(self, ranges=None, **kwargs):
          
        range_list  = self.probs.keys()

        if ranges:
            range_list = ranges
        range_list = sorted(range_list)

        for k in range_list:
            for variable in self.__variables[k].keys():
                self.check_variable_bounds(k, variable)
              
#             status = self.probs[k].solve(self.get_solver(self.solver_option), **kwargs)

            try:
                status = self.probs[k].solve(self.get_solver(self.solver_option), **kwargs)
            except:
                status = -1
                print('problem to solve {}'.format(k))

            if status == -1:
                logging.critical('{}: unfeasible'.format(k, status))
                self.create_lp_file(self.probs[k], k)

            else:
                logging.critical('{}: optimal'.format(k, status))
                self.create_solver_result(k)

    def create_lp_file(self, prob, key):

        if not(os.path.exists('us8/lpfiles')):
            os.mkdir('us8/lpfiles')
        prob.writeLP(os.path.join('us8/lpfiles', key.replace(
            'restricoes', 'modelo') + '.lp'))
        
    def create_lp_file2(self, prob, key):

        if not(os.path.exists('lpfiles')):
            os.mkdir('lpfiles')
        prob.writeLP(os.path.join(self.path_to_constraints,'lpfiles', key.replace(
            'restricoes', 'modelo') + '.lp'))                

    def get_solver(self, solver_option):
        return self.solvers.get(solver_option, pulp.COIN())

    def create_solver_result(self, key):
        dict_results = {}
        dict_results[key] = {}

        prob = self.probs[key]
        
#         for k,v in self.__variables[key].items():
#             print("aki ",k)
#             print("aki2 ",v)
#             str(v['obj'])
#             print("--------------------")
          
        inv_variables = {str(v['obj']): k for k,
                         v in self.__variables[key].items() if len(k) > 0}
        
        for variable in prob.variables():
            variable_name = inv_variables.get(str(variable))
            dict_results[key][variable_name] = {}

            variable_min = self.__variables[key][inv_variables[str(
                variable)]]['min'] if 'min' in self.__variables[key][variable_name] else 0

            variable_max = self.__variables[key][inv_variables[str(
                variable)]]['max'] if 'max' in self.__variables[key][variable_name] else 0


            if self.df_obj_function.loc[self.df_obj_function['TAG'] ==
                                                variable_name, 'Custo'].shape[0] > 1:
                print(self.df_obj_function.loc[self.df_obj_function['TAG'] ==
                                                    variable_name, 'Custo'].shape)

            obj_coef = self.df_obj_function.loc[self.df_obj_function['TAG'] ==
                                                variable_name, 'Custo'].values[0] if variable_name in self.df_obj_function['TAG'].tolist() else 0

            dict_results[key][variable_name][' VariableName'] = variable_name
            dict_results[key][variable_name][' LB'] = float(variable_min)
            dict_results[key][variable_name][' UB'] = float(variable_max)
            dict_results[key][variable_name][' ObjCoeff'] = obj_coef
            dict_results[key][variable_name][' Value'] = variable.varValue


#             dict_result[key]

        self.dict_results_ = dict_results
        self.df_results[key] = pd.DataFrame.from_dict(dict_results[key]).T
        self.df_results[key].sort_values(by=[' ObjCoeff'], ascending=[False], inplace=True)


    def define_optimization_costs(self, scalers, unnormalize_features, plant_map, remove_tags=None):

        file_name = 'Variables - VarX_{}-{}.csv'.format

        real_costs = self.__costs.copy()

        columns = [' Value', ' ObjCoeff', ' LB', ' UB']

        flag_first = True

        for k, df in self.df_results.items():

            # file = file_name(range_min, range_max)

            if self.probs[k].status != 1:

                logging.warn("No solution to range ", k)

                continue

            # df = pd.read_csv(os.path.join(solver_path, file), sep=';', encoding='iso-8859-1')

            df = df.to_dict('records')

            range_cost = 0 # the cost of the current range

            results = []

            for var_df in df:

                # var_df[' VariableName'] = PulpSolver.parse_variables_name([var_df[' VariableName']])
                var = var_df[' VariableName']

                # for column in columns:

                #     var_df[column] = PulpSolver.parse_coeficients([var_df[column]])

                need_unnormalize = var in scalers and var not in self.constants.TARGETS_IN_MODEL

                if need_unnormalize and 'CONS1_Y@07QU-VT0' not in var and var not in unnormalize_features:

                    var_df = apply_normalization(var_df, scalers)

                elif var in self.constants.TARGETS_IN_MODEL:

                    if var in scalers:

                        var_df[" ObjCoeff"] = operations.normalize_feature(scalers, var, var_df[' Value'])


                var_df['custo'] = 0

                if var in real_costs.keys():

                    range_cost += var_df[' Value'] * real_costs[var]

                    var_df['custo'] = real_costs[var]

                var_df['range'] = (k)


                if var_df[' VariableName'] in plant_map: # converting names
                    if plant_map[var_df[' VariableName']] == 'NÃO EXISTE':
                        print(var_df[' VariableName'])
                    var_df[' VariableName'] = plant_map[var_df[' VariableName']]

            print('Production Range: {} - Cost: {:.5f}'.format(k, range_cost))

            df = rename_columns(df)

            results.append(df)

            # save_solver_results(pd.DataFrame(df), flag_first, remove_tags)

            if flag_first:
                df_output = pd.DataFrame(df)
            else:
                df_output = pd.concat([df_output, pd.DataFrame(df)])

            flag_first = False
            if df_output is not None:
                self.__df_output = df_output
                self.__raw_columns = df_output.columns
                self.stepsValidator = StepsValidator(df_output.copy(), df_output.columns)


        return df_output


    def extract_key_name(self, file_path):
        pattern = re.compile('\\d+-\\d+')
        search = pattern.search(file_path)
        key = search.group()

        return key

    def export_solution(self):
        try:
            df_output = self.__df_output[['faixa', 'TAG', 'minimo', 'maximo', 'valor normalizado', 'valor real', 'custo']]
            df_output.round(5).to_csv('resultado_otimizador/resultado_otimizador-formato-antigo.csv', sep=';',
             encoding='iso-8859-1', index=False, decimal=',')
            logging.info('results exported to path: "{}"'.format(os.path.join('resultado_otimizador/resultado_otimizador-formato-antigo.csv')))
        except:
            raise Exception('Problem to export results')


    def remove_non_ascii_normalized(self, string: str) -> str:
      normalized = unicodedata.normalize('NFD', string)
      return normalized.encode('ascii', 'ignore').decode('utf8')

    def sub_specfic_caracters(self, sentence):
        fixed_sentence = sentence.replace(
            '*', 'mult').replace('=', 'equal').replace('/', 'div')
        
        fixed_sentence = self.remove_non_ascii_normalized(fixed_sentence)
        
        return fixed_sentence

    def get_variables(self):
        return self.__variables

    def get_solver_option(self):
        return self.solver_option_

    def set_solver_option(self, solver_option):
        self.solver_option_ = solver_option

    def get_range_results(self):
        return self.df_results

    def get_dict_results(self):
        return self.dict_results_

    def get_variable_optimal_result(self, key, variable):
        return self.dict_results[key][variable]

    def get_probs(self):
        return self.probs

    def export_results(self):
        for file in glob.glob(os.path.join(self.path_to_constraints,"Variables - VarX_*.csv")):
            os.remove(file)
        for k, result in self.df_results.items():
            export_file_name = 'Variables - VarX_{}.csv'.format(k)
            export_file_path = os.path.join(self.path_to_constraints, export_file_name)

                
            result.to_csv(export_file_path, sep=';', index=False, encoding="ISO-8859-1")

    @staticmethod
    def parse_variables_name(variables_name):
        try:
	        return list(map(lambda var: re.search(r'\((.*)\)', var).group(1), variables_name))[0]
        except: raise Exception(variables_name)

    @staticmethod
    def parse_coeficients(coeficients):
	    return list(map(lambda coef: float(coef.replace(',', '.')), coeficients))[0]

    @staticmethod
    def apply_normalization(var_df, scalers):

        columns = [' Value', ' LB', ' UB']

        var_df[" ObjCoeff"] = var_df[' Value']

        for column in columns:

            var_df[column] = operations.unnormalize_feature(scalers, var_df[' VariableName'], var_df[column], 'one_side')

        return var_df


