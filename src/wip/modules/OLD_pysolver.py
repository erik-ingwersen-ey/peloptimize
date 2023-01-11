import logging


logging.war


import abc
import pulp
import json
import pandas as pd
import  unicodedata
import logging

class PySolver():
    DEFAULT_LOGICAL_OPERATORS_KEY = ['GT', 'LT', 'LTE', 'GTE', 'E']

    def __init__(self, solver_option='cbc'):
        self.__solvers = {
            'cplex': pulp.CPLEX(),
            'gurobi': pulp.GUROBI(),
            'glpk': pulp.GLPK(),
            'cbc':  None,
        }

        self.__solver_option = solver_option
        self.__prob = None


    def define_problem(self, obj_function: list, constraints: dict, limits=None, minimize=True) -> None:


        # define a dict witch will store all pulp variables.
        self.define_variables_limits(limits)

        #add constraints to a LP problem
        self.define_constraints_problem(constraints=constraints, problem_name='pelletizing')

        #define objective function
        self.define_objective_function(obj_function)

        


    
    def mount_plus(self):
        pass
    
    def mount_multiply(self):
        pass

    def remove_non_ascii_normalized(self, string: str) -> str:
        normalized = unicodedata.normalize('NFD', string)
        return normalized.encode('ascii', 'ignore').decode('utf8')
    
    def sub_specfic_caracters(self, sentence):
        fixed_sentence = sentence.replace(
            '*', 'mult').replace('=', 'equal').replace('/', 'div')

        fixed_sentence = self.remove_non_ascii_normalized(fixed_sentence)
        
        return fixed_sentence

    def define_variables_limits(self, limits: dict) -> None:
        
        pulp_variables = {}
        #creating and setting variables limits
        for variable, variable_limit in limits.items():
            
            #Variable is a func variable than the LPVariable type must be binary
            parsed_variable_name = self.sub_specfic_caracters(variable)
            pulp_variable = pulp.LpVariable(parsed_variable_name, lowBound=variable_limit['min'], upBound=variable_limit['max'], cat=variable_limit['type'])
        
            pulp_variables[variable] = pulp_variable
        
        self.__variables = pulp_variables


    def define_constraints_problem(self, constraints: dict, problem_name, minimize=True) -> None:
        
        pulp_constraints = {}
        
        problem_type = pulp.LpMinimize if minimize else pulp.LpMinimize

        prob = pulp.LpProblem(problem_name, problem_type)
        # creating pulp constraints based on constraints dict

        for constraint_name, constraint_terms in constraints.items():

            pulp_constraint = pulp.LpConstraint()

            for constraint_term in constraint_terms:
                
                if len(constraint_term) == 2 and constraint_term[0] not in self.DEFAULT_LOGICAL_OPERATORS_KEY:
                    if constraint_term[0] not in self.__variables:
                        logging.warning('{} not definied, creating a default variable'.format(constraint_term[0]))
                        pulp_var_name = self.sub_specfic_caracters(constraint_term[0])
                        self.__variables[constraint_term[0]] = pulp.LpVariable(pulp_var_name, cat=self.get_variable_type(constraint_term[0]))
                    
                    pulp_constraint += self.__variables[constraint_term[0]] * constraint_term[1]
                    

                    
                    
                                    
                elif len(constraint_term) == 1:
                    pulp_constraint += constraint_term[0]
                
                else:
                    pulp_constraint = self.set_constraint_logical_operator(pulp_constraint, constraint_term[0], constraint_term[1])

            pulp_constraint.setName(self.sub_specfic_caracters(constraint_name))
            pulp_constraints[constraint_name] = pulp_constraint
            prob += pulp_constraint
        
        self.__prob = prob
        self.__constraints = pulp_constraints

    def define_objective_function(self, obj_function):
        '''
        Define the objective Function

        
        '''
        print(obj_function)
        obj_list = [ self.__variables.get(var, pulp.LpVariable(var)) * coef for var, coef in obj_function]
        pulp_obj_function =  pulp.lpSum(obj_list)
        
        self.__obj_function =  pulp_obj_function
        self.__prob += pulp_obj_function


        self.__prob.writeLP('teste.lp')

            # for term in constraint_term 


    def set_constraint_logical_operator(self, pulp_constraint: pulp.LpConstraint, comparator: str, value: float) -> pulp.LpConstraint:
        '''
        Define logical operator to a constraint
        '''
        
        if comparator == 'LT' or comparator == 'LTE':
            pulp_constraint = pulp_constraint <= value 
        
        elif comparator == 'GT' or comparator == 'GTE':
            pulp_constraint = pulp_constraint >= value 
        
        else:
            pulp_constraint = pulp_constraint == value

        return pulp_constraint

    def check_vars_bound(self)->list:
        no_bounded_variables = {var: pulp_var for var, pulp_var in self.__variables.items() if pulp_var.lowBound is None or pulp_var.upBound is None }
        return no_bounded_variables
        
    def get_variable(self, variable_name):
        return self.__variables[variable_name]
    
    def get_variables(self):
        return self.__variables

    def get_prob(self):
        return self.__prob

    
    def get_constraints(self):
        pass

    def get_constraint(self):
        pass

    def set_constraint(self):
        pass

    def get_solver_option(self):
        pass

    def get_variables_limit(self):
        pass

    def set_variable_ub(self):
        pass

    def set_variable_lb(self):
        pass

    def get_solver_option(self):
        return self.__solver_option

    def export_results(self):
        pass

    
    @abc.abstractmethod
    def parser_file(self):
        pass

    @abc.abstractmethod
    def get_variable_type(variable_name: str)->str:
        pass

class PelletizingPySolver(PySolver):
    
    def get_variable_type(self, variable_name: str) -> str:
            '''
            Returna  pulp variable type key string

            Parameters:
                variable_name (string): variable name

            Returns:
                str: Returning a str key variable type
            '''
            
            if 'FUNC' in variable_name:
                variable_type = 'Binary'

            elif 'qtde' in variable_name or 'SOMA FUNC' in variable_name or 'SOMA_FUNC' in variable_name:
                variable_type = 'Integer'
                
            else:
                variable_type = 'Continuous'
        
            return variable_type


    def parser_file(self):
        

        def parse_coefficient(coef):
            return float(coef.strip().replace(',', '.'))

        def get_bound_specification(constraint_name):
            return 'min' if 'min' in constraint_name else 'max'

        f_objective = {}
        constraints = {}
        limits = {}

        default_structure = {}
        with open('us8/restricoes-faixa-750-800.txt') as fp:

            for line in fp.readlines():
                
                line = line.replace('\n', '')
                tokens = line.split(';')
                constraint_name = tokens.pop(0)
                tokens = list(map(str.lstrip, tokens))


                # line with descritption
                if len(tokens) > 2:
                    __ = tokens.pop(-1)
                    tokens[1] = parse_coefficient(tokens[1])
                
                # line with feature and a coef
                elif len(tokens) == 2:
                    tokens[1] = parse_coefficient(tokens[1])

                #line with logical operator and a coef
                elif len(tokens) == 1 and len(tokens[0].split()) == 2:
                    tokens = tokens[0].split()
                    tokens[1] = parse_coefficient(tokens[1])
                
                #line with a single value
                else:
                    tokens[0] = parse_coefficient(tokens[0])
                

                if constraint_name not in default_structure:
                    default_structure[constraint_name] = []

                    
                default_structure[constraint_name].append(tuple(tokens))

                # if len(conditional_operator) == 2 and conditional_operator[0]:


                if 'PESO1_I@08PE-BW-840I-01M1' in constraint_name:
                    print(constraint_name)

                if '_limit' in constraint_name:
                    bound_specification = 'min' if 'min' in constraint_name else 'max'

                    if len(tokens) == 2 and tokens[0] not in PelletizingPySolver.DEFAULT_LOGICAL_OPERATORS_KEY:
                        variable_name = tokens[0]
                        if variable_name not in limits:
                            limits[variable_name] = {}
                            limits[variable_name]['type'] = self.get_variable_type(variable_name)


                    elif len(tokens) == 1 and bound_specification not in limits[variable_name].keys():
                        limits[variable_name][bound_specification] = -1 * tokens[0]
                    
                    elif bound_specification not in limits[variable_name] and tokens[0] in PelletizingPySolver.DEFAULT_LOGICAL_OPERATORS_KEY:
                        limits[variable_name][bound_specification] =  tokens[1]


                elif 'min' in constraint_name or 'max' in constraint_name :
                    bound_specification = get_bound_specification(constraint_name)

                    if type(tokens[0]) == str and ('FUNC' in tokens[0]):
                        limits[variable_name][bound_specification] = -1 * tokens[1]

                    elif  type(tokens[0]) == float:
                        limits[variable_name][bound_specification] = -1 * tokens[0]

                    elif len(tokens) == 2 and tokens[0] not in PelletizingPySolver.DEFAULT_LOGICAL_OPERATORS_KEY:
                        variable_name = tokens[0]
                        if variable_name not in limits:
                            limits[variable_name] = {}
                            limits[variable_name]['type'] = self.get_variable_type(variable_name)     
                
            constraints = default_structure

   

        df_obj_function = pd.read_csv('us8/costs.csv', sep=';')
        df_obj_function['Custo'] = df_obj_function['Custo'].apply(
            lambda x: float(x.replace(',', '.')))
        
        obj_function = [(row['TAG'], row['Custo']) for idx, row in df_obj_function.iterrows()]

        return obj_function, constraints, limits

    
pelletizingSolver = PelletizingPySolver()
obj_function, constraints, limits = pelletizingSolver.parser_file()

with open('constraints.json', 'w') as fp:
    json.dump(constraints, fp)
with open('limits.json', 'w') as fp:
    json.dump(limits, fp)
with open('objective_function.txt', 'w') as fp:
    fp.write(' '.join(str(s) + '\n' for s in obj_function) + '\n')


print(limits['PESO1_I@08PE-BW-840I-01M1'])
# for var, coef in obj_function:
#     print(var, constraints.get(var))
pelletizingSolver.define_problem(obj_function, constraints, limits, minimize=True)
# status = pelletizingSolver.get_prob().solve()
# print(status)

# print(pelletizingSolver.get_prob())


# if __name__ == '__main__':
#     main()


        


pelletizingSolver.get_prob().solve()


pelletizingSolver.check_vars_bound()


for k, v in limits.items():
    if 'min' not in v.keys() or 'max' not in v.keys():
        print(k)


pelletizingSolver.get_variable('cfix')


