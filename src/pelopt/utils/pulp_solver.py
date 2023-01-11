import glob
import json
import os
import re
import unicodedata

import pandas as pd
import pulp

from pelopt.utils import operations
from pelopt.utils.logging_config import logger


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
        """Exporta o Dataframe contendo os testes aplicados aos resultados."""

        df_debug = self.__df_debug

        condition = df_debug.loc[:,
                    ~df_debug.columns.isin(self.__raw_columns.tolist())].sum(
            1) == 1
        ixs = df_debug.loc[condition, :].index

        writer = pd.ExcelWriter('resultado_otimizador/debug.xlsx',
                                engine='xlsxwriter')
        self.__df_debug.loc[:, self.__raw_columns.tolist()].to_excel(
            writer, sheet_name='debug'
        )
        workbook = writer.book

        # estilo da cor que marca as linhas que respeitam os testes
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
        validate = ~((df_debug['minimo'] <= df_debug['valor real']) | (
            df_debug['maximo'] >= df_debug['valor real']))
        validate = pd.Series(validate, name=self.getTestName())
        return validate


class PulpSolver:
    """
    solver_option: 'cbc', 'gurobi', 'glpk', 'cplex'. Default is 'cbc'
    """
    def __init__(self, path_to_constraints, path_to_costs, solver_option=None):
        self.__variables = {}
        self.probs = {}  # Texto do arquivo lp
        self.df_results = {}
        self.solver_option = solver_option
        self.path_to_constraints = path_to_constraints
        self.path_to_costs = path_to_costs
        self.constraints = {}
        self.logs = ''

        self.solvers = {
            'cplex': pulp.CPLEX(),
            'gurobi': pulp.GUROBI(),
            'glpk': pulp.GLPK(),
            'cbc': None,
        }
        # O `costs.csv` é gerado em `outputs` e é advindo de
        # `custo_real.json`, gerado em `Otimizacao` sendo advindo de
        # `files/custo_real`
        path_to_objective_function = os.path.join(
            path_to_constraints, 'costs.csv'
        )

        df_obj_function = pd.read_csv(path_to_objective_function, sep=';')
        df_obj_function['Custo'] = df_obj_function['Custo'].apply(
            lambda x: float(x.replace(',', '.')))

        with open(path_to_costs, 'r') as fp:
            self.__costs = json.load(fp)

        self.df_obj_function = df_obj_function

        for file_path in glob.glob(
            os.path.join(path_to_constraints, 'restricoes-faixa-*-*.txt')):
            self.filepath = file_path
            key = self._extract_key_name(file_path)
            self._parser_restriction_files(file_path, key)
            self._parse_objective_file(key)
            logger.info('Done!')

    def _parser_restriction_files(self, file_path, key):
        with open(file_path, mode='r', encoding='utf-8') as fp:
            sentences = fp.read().splitlines()

        self.probs[key] = pulp.LpProblem(key, pulp.LpMinimize)

        self.__variables[key] = {}
        constraint_old_name = sentences[0].split(';')[0]
        constraint = pulp.LpConstraint()
        bound_value = None
        for i in range(1, len(sentences)):
            # terms = cada linha das restrições-faixa
            terms = sentences[i].split(';')
            logger.debug('\r\nterms: %s', terms)

            constraint_name = terms.pop(0)  # modelo, ex: abrasão
            if 'Consumo' in constraint_name:
                constraint_name = constraint_name  # Como assim?

            if constraint_name != constraint_old_name:  # se mudou para novo
                if constraint_old_name in self.probs[key].constraints:
                    constraint.setName(self._sub_specfic_characters(
                        constraint_old_name + '_v2'))
                else:
                    constraint.setName(
                        self._sub_specfic_characters(constraint_old_name))

                if (
                    constraint.getName() not in self.probs[key].constraints
                    and 'limit_' not in constraint.getName()
                ):
                    self.probs[key] += constraint
                    self.constraints[constraint.getName()] = constraint

                elif 'limit_' not in constraint.getName():
                    logger.warning(
                        'Constraint "%s" already in lp problem, '
                        'skipping it.', constraint.getName()
                    )
                constraint = pulp.LpConstraint()
                constraint_old_name = constraint_name

            if 2 <= len(terms) <= 3:
                feature, coef = terms[0].lstrip(), terms[1]

                constraint = self._mount_multiply(feature, coef,
                                                  constraint, key)
                if (
                    '_limit_' not in constraint_name
                    and '_limit_' not in constraint_old_name
                    and ('min' in constraint_name or 'max' in constraint_name)
                ):
                    if 'FUNC' not in feature:
                        feature_to_set_limit = feature
                    else:
                        bound_value = float(coef.replace(',', '.'))

                    if feature_to_set_limit and bound_value:
                        self._save_variable_limits(bound_value, constraint_name,
                                                   key, feature_to_set_limit)
                        feature_to_set_limit = None
                        bound_value = None
            elif len(terms) == 1:
                coef = terms[0].split()
                if len(coef) == 1:
                    constraint, value = self._mount_add(coef, constraint)
                    if '_limit_' in constraint_name:
                        self._save_variable_limits(
                            value, constraint_name, key, feature)
                else:
                    constraint, value = self._mount_comparison(
                        coef, constraint)

    def _parse_objective_file(self, key):
        for k, v in self.probs.items():
            variables_not_created = self.df_obj_function.loc[
                ~self.df_obj_function['TAG'].isin(
                    self.__variables[key].keys())]
            variables_not_created['TAG'].apply(
                lambda x: self._add_variable(x, key))

            obj = pulp.lpSum(
                [self.__variables[key][name]['obj'] * cost for name, cost in
                 zip(
                     self.df_obj_function['TAG'],
                     self.df_obj_function['Custo'])]), 'objective_function'
            self.probs[key] += obj

    def _add_variable(self, feature, key):
        self.__variables[key][feature] = {}
        simple_feature_name = self._sub_specfic_characters(feature)
        if feature.startswith('FUNC') or feature.startswith('status'):
            self.__variables[key][feature]['obj'] = pulp.LpVariable(
                simple_feature_name, cat="Binary", lowBound=0, upBound=1)

            self.__variables[key][feature]['min'] = 0
            self.__variables[key][feature]['max'] = 1

        elif any(
            feature.startswith(substr) for substr in ['SOMA FUNC', 'qtde',
                                                      'SOMA_FUNC']
        ):
            self.__variables[key][feature]['obj'] = pulp.LpVariable(
                simple_feature_name, cat="Integer")

        else:
            self.__variables[key][feature]['obj'] = pulp.LpVariable(
                simple_feature_name)

    def _mount_multiply(self, feature, coef, constraint, key):
        try:
            coef = float(coef.replace(',', '.'))
        except Exception as err:
            raise ValueError(
                f'Erro de conversão de tipo valor: {coef} - Feature: {feature} '
                f'- Nome da restrição: {constraint}'
            ) from err

        if feature not in self.__variables[key]:
            self._add_variable(feature, key)
        constraint += self.__variables[key][feature]['obj'] * coef
        return constraint

    def _mount_add(self, coef, constraint):
        value = float(coef[0].replace(',', '.'))
        constraint = constraint + value

        return constraint, value

    def _mount_comparison(self, coef, constraint, errors: str = 'ignore'):
        logical_operator = coef[0]
        value = float(coef[1].replace(',', '.'))
        if 'GT' in logical_operator:
            constraint = constraint >= value
        elif 'LT' in logical_operator:
            constraint = constraint <= value
        elif logical_operator in ['E', 'EQ']:
            constraint = constraint == value
        elif errors == 'raise':
            raise ValueError(f'Invalid Logical operator: "{logical_operator}"')
        return constraint, value

    def _save_variable_limits(self, value, constraint_name, key, feature):
        limit_key = 'min' if constraint_name.endswith('min') else 'max'
        if limit_key == 'min':
            self.__variables[key][feature]['obj'].lowBound = -1 * value
        else:
            self.__variables[key][feature]['obj'].upBound = -1 * value
        self.__variables[key][feature][limit_key] = -1 * value

    def _check_variable_bounds(self, key, feature):
        if 'min' not in self.__variables[key][feature]:
            self.__variables[key][feature]['min'] = 0
            self.__variables[key][feature]['obj'].lowBound = 0
        if 'max' not in self.__variables[key][feature]:
            self.__variables[key][feature]['max'] = 1
            self.__variables[key][feature]['obj'].upBound = 1

    def solve_range(self, ranges=None, tmp_path=f'/dbfs/tmp/us_not_defined',
                    us=0, **kwargs):
        range_list = self.probs.keys()
        if ranges:
            range_list = ranges
        range_list = sorted(range_list)

        for k in range_list:
            for variable in self.__variables[k].keys():
                self._check_variable_bounds(k, variable)
            try:
                status = self.probs[k].solve(
                    self._get_solver(self.solver_option), **kwargs)
            except Exception as err:
                status = -1
                logger.exception(err)
                logger.info('Failed to optimize range: %s', k)
            if status == 1:
                logger.info('%s: %s', k, pulp.constants.LpStatus[status])
                self._create_solver_result(k)
            else:
                logger.critical('%s: %s', k, pulp.constants.LpStatus[status])
            self.create_lp_file(self.probs[k], k, tmp_path, us)

    def create_lp_file(self, prob, key, tmp_path, us):
        logger.info(f'building lp file for plant: {us}')
        if not (os.path.exists(f'{tmp_path}/lpfiles')):
            os.mkdir(f'{tmp_path}/lpfiles')
        prob.writeLP(
            os.path.join(
                f'{tmp_path}{os.path.sep}lpfiles',
                key.replace('restricoes', 'modelo') + '.lp'
            )
        )

    def _get_solver(self, solver_option):
        return self.solvers.get(solver_option, pulp.COIN())

    def _create_solver_result(self, key):
        dict_results = {key: {}}

        prob = self.probs[key]
        inv_variables = {
            str(v['obj']): k for k, v in self.__variables[key].items()
            if len(k) > 0
        }

        for variable in prob.variables():
            variable_name = inv_variables.get(str(variable))
            dict_results[key][variable_name] = {}

            variable_min = self.__variables[key][inv_variables[str(
                variable)]]['min'] if 'min' in self.__variables[key][
                variable_name] else 0

            variable_max = self.__variables[key][inv_variables[str(
                variable)]]['max'] if 'max' in self.__variables[key][
                variable_name] else 0

            obj_coef = self.df_obj_function.loc[self.df_obj_function['TAG'] ==
                                                variable_name, 'Custo'].values[
                0] if variable_name in self.df_obj_function[
                'TAG'].tolist() else 0

            dict_results[key][variable_name][' VariableName'] = variable_name
            dict_results[key][variable_name][' LB'] = float(variable_min)
            dict_results[key][variable_name][' UB'] = float(variable_max)
            dict_results[key][variable_name][' ObjCoeff'] = obj_coef
            dict_results[key][variable_name][' Value'] = variable.varValue

        self.dict_results_ = dict_results
        self.df_results[key] = pd.DataFrame.from_dict(dict_results[key]).T
        self.df_results[key].sort_values(by=[' ObjCoeff'], ascending=[False],
                                         inplace=True)

    def _extract_key_name(self, file_path):
        pattern = re.compile('\\d+-\\d+')
        search = pattern.search(file_path)
        key = search.group()

        return key

    def _remove_non_ascii_normalized(self, string: str) -> str:
        normalized = unicodedata.normalize('NFD', string)
        return normalized.encode('ascii', 'ignore').decode('utf8')

    def _sub_specfic_characters(self, sentence):
        fixed_sentence = sentence.replace(
            '*', 'mult').replace('=', 'equal').replace('/', 'div')
        fixed_sentence = self._remove_non_ascii_normalized(fixed_sentence)

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
        for file in glob.glob(
            os.path.join(self.path_to_constraints, "Variables - VarX_*.csv")
        ):
            os.remove(file)
        for k, result in self.df_results.items():
            export_file_name = 'Variables - VarX_{}.csv'.format(k)
            export_file_path = os.path.join(self.path_to_constraints,
                                            export_file_name)
            result.to_csv(export_file_path, sep=';', index=False,
                          encoding="ISO-8859-1")

    @staticmethod
    def parse_variables_name(variables_name):
        # logger.info(f'parse_variables_name: {variables_name}', )
        try:
            return list(map(lambda var: re.search(r'\((.*)\)', var).group(1),
                            variables_name))[0]
        except:
            raise Exception(variables_name)

    @staticmethod
    def parse_coeficients(coeficients):
        return list(map(lambda coef: float(coef.replace(',', '.')),
                        coeficients))[0]

    @staticmethod
    def apply_normalization(var_df, scalers):
        columns = [' Value', ' LB', ' UB']
        var_df[" ObjCoeff"] = var_df[' Value']

        for column in columns:
            var_df[column] = operations.unnormalize_feature(
                scalers, var_df[' VariableName'], var_df[column], 'one_side'
            )
        return var_df
