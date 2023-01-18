import numpy as np
import importlib

import wip.files.complex_constraints
import wip.files.general_constraints
import wip.files.range_complex_constraints
import wip.files.range_constraints
import wip.files.variable_constraints


class Constraints:
    """Defining the problem constraints."""

    @staticmethod
    def write_feature_constraints(feature, file, lmin, lmax):

        solver_operations.write_constraint(
            file, feature + "_limit_min", [(feature, 1), -lmin, ("GTE", 0)]
        )
        solver_operations.write_constraint(
            file, feature + "_limit_max", [(feature, 1), -lmax, ("LTE", 0)]
        )

    @staticmethod
    def read_constraints(file="general_constraints.json", parse=True):
        base_path = (
            "abfss://insight@usazu1valesa001.dfs.core.windows.net"
            "/Workarea/Pelletizing/Process_Optimization/Usina08"
            "/Otimizacao/files/"
        )
        data = operations.read_json_dls(base_path, file)
        if not parse:
            return data

        # if value is list return tuple in case value is not list return the
        # value
        new_data = {}
        for key, value in data.items():
            new_data[key] = [
                tuple(operation) if isinstance(operation, list) else operation
                for operation in value
            ]
        return new_data

    @staticmethod
    def parse_data(data):
        new_data = {}
        for key, value in data.items():
            new_data[key] = [
                tuple(operation) if isinstance(operation, list) else operation
                for operation in value
            ]
        return new_data

    @staticmethod
    def write_simple_constraints(file):
        """Write constraints that are constant and each range."""
        new_data = Constraints.parse_data(general_constraints)
        for constraint, operation in new_data.items():
            solver_operations.write_constraint(file, constraint, operation)

    @staticmethod
    def write_targets_limits(file, datasets, features_limits):

        """Write constraints to targets of all models."""

    targets = list(map(lambda x: datasets[x].columns[-1], datasets))
    targets_write = list(filter(lambda x: x not in features_limits, targets))
    select_dataset = {
        target: list(filter(lambda x: target in datasets[x], datasets))
        for target in set(targets_write)
    }
    for target in set(targets_write):
        values = datasets[select_dataset[target][0]][target]
        if target == "0":
            continue
        if target in constants.TARGETS_IN_MODEL.keys():
            new_target = constants.TARGETS_IN_MODEL[target]
            Constraints.write_feature_constraints(
                new_target, file, min(values), max(values)
            )
            Constraints.write_feature_constraints(
                target, file, min(values), max(values)
            )
        else:
            Constraints.write_feature_constraints(
                target, file, min(values), max(values)
            )

    @staticmethod
    def define_range_terms(range_terms, scalers):
        range_constraints = solver_operations.define_range_constraints
        if not isinstance(range_terms, list):
            range_terms = [range_terms]
        all_terms = []
        for terms in range_terms:
            step = 1
            if "step" in terms.keys():
                step = terms["step"]
            parsed_terms = range_constraints(
                terms["feature"], terms["start"], terms["end"] + 1, step
            )
            norm_features = []
            if "norm_feature" in terms.keys():
                norm_features = range_constraints(
                    terms["norm_feature"], terms["start"], terms["end"] + 1,
                    step,
                )
            term = terms.copy()
            for index, feature in enumerate(parsed_terms):
                term["feature"] = feature
                if norm_features:
                    _, new_coef = Constraints.measure_new_coef(
                        term, scalers, norm_features[index]
                    )
                else:
                    _, new_coef = Constraints.measure_new_coef(term, scalers)
                all_terms.append((feature, new_coef))
        return all_terms

    @staticmethod
    def write_simple_range_terms(file, scalers, features_limits):
        range_terms = range_constraints
        for constraint_name, terms in range_terms.items():
            terms = Constraints.parse_complex_constraints(
                terms, features_limits, scalers
            )
            solver_operations.write_constraint(file, constraint_name, terms)

    @staticmethod
    def define_ordinary_constraints(terms):
        return [(term["feature"], term["coef"]) for term in terms]

    @staticmethod
    def parse_complex_constraints(
        terms, features_limits, scalers, range_min=None, range_max=None
    ):
        simple_constraint = []
        for term in terms:
            if (
                len(term.keys()) == 2
                and "feature" in term.keys()
                and "coef" in term.keys()
            ):
                simple_constraint.append(tuple(term.values()))
            elif "limit" not in term.keys() and "operator" in term.keys():
                operator = tuple(term.values())
            elif "limit" in term.keys() and "operator" not in term.keys():
                limit = Constraints.define_term_limit(
                    term, features_limits, range_min, range_max
                )
                simple_constraint.append((term["feature"], limit))
            elif "limit" in term.keys() and "operator" in term.keys():
                if term["operator"] == "norm":
                    method_operator = operations.normalize_feature
                limit = Constraints.define_term_limit(
                    term, features_limits, range_min, range_max
                )
                simple_constraint.append(
                    (
                        term["coef"]
                        * method_operator(scalers, term["feature"], limit)
                    )
                )
            elif "start" in term.keys() and "end" in term.keys():
                simple_constraint.extend(
                    Constraints.define_range_terms(term, scalers)
                )
        simple_constraint.append(operator)
        return simple_constraint

    @staticmethod
    def define_term_limit(term, features_limits, range_min, range_max):
        if term["limit"] == "fmin":
            limit = range_min
        elif term["limit"] == "fmax":
            limit = range_max
        else:
            limit = features_limits[term["feature"]][term["limit"]]
        return limit

    @staticmethod
    def write_variable_constraints(
        file, features_limits, scalers, range_min, range_max
    ):
        """Write complex constraints with function as: norm, lmin, lmax."""
        constraints = variable_constraints
        constraints_temp = constraints.copy()

        for k, constraint in constraints.items():
            for sentence in constraint:
                if (
                    "feature" in sentence
                    and sentence["feature"] not in features_limits.keys()
                ):
                    constraints_temp.pop(k)
                    break

        constraints = constraints_temp
        # for constraint, terms in constraints.items():
