"""
Define the optimization problem constraints.
"""
from pelopt.utils.operations import (
    normalize_feature,
    read_json,
    unnormalize_feature,
)
from pelopt.utils.solver_operations import solver_operations


def write_feature_constraints(
    feature, file, lmin, lmax, solver_operations=None
):
    solver_operations.write_constraint(
        file, feature + "_limit_min", [(feature, 1), -float(lmin), ("GTE", 0)]
    )
    solver_operations.write_constraint(
        file, feature + "_limit_max", [(feature, 1), -float(lmax), ("LTE", 0)]
    )


def read_constraints(
    file: str = "files/general_constraints.json", parse: bool = True
):
    data = read_json(file)
    if not parse:
        return data
    return parse_constraints(data)


def parse_constraints(data: dict) -> dict:
    """Parse the constraints to a format that the solver can use."""
    return {
        key: [
            tuple(operation) if isinstance(operation, list) else operation
            for operation in value
        ]
        for key, value in data.items()
    }


def write_simple_constraints(file):
    """Write constraints that are constant at each range."""
    new_data = parse_constraints(general_constraints.copy())
    for constraint, operation in new_data.items():
        solver_operations.write_constraint(file, constraint, operation)


def write_features_limits(
    constraint_files,
    scalers,
    feature,
    lmin,
    lmax,
    features_limits,
    denormalize_features,
):
    inv_ = INV_TARGETS_IN_MODEL
    if feature in scalers and "func" not in feature.lower():
        denormalized_lmin = unnormalize_feature(
            scalers, feature, lmin, "one_operation"
        )
        denormalized_lmax = unnormalize_feature(
            scalers, feature, lmax, "one_operation"
        )

    elif feature in denormalize_features:
        limits = get_features_to_unnormalize(feature)
        denormalized_lmin = unnormalize_feature(
            scalers, feature, limits["lmin"], "one_operation"
        )
        denormalized_lmax = unnormalize_feature(
            scalers, feature, limits["lmax"], "one_operation"
        )
    else:
        raise ValueError(
            f'Feature "{feature}" not found in `scalers` or '
            f"`denormalize_features`."
        )
    if feature in TARGETS_IN_MODEL.keys() or feature in denormalize_features:
        write_feature_constraints(
            feature, constraint_files, denormalized_lmin, denormalized_lmax
        )
    else:
        write_feature_constraints(feature, constraint_files, lmin, lmax)
    features_limits[feature] = {"lmin": lmin, "lmax": lmax}

    if feature in TARGETS_IN_MODEL.values():
        write_feature_constraints(
            inv_[feature],
            constraint_files,
            unnormalized_lmin,
            unnormalized_lmax,
        )
        features_limits[inv_[feature]] = {"lmin": lmin, "lmax": lmax}

    return features_limits


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
                terms["norm_feature"], terms["start"], terms["end"] + 1, step
            )
        term = terms.copy()
        for index, feature in enumerate(parsed_terms):
            term["feature"] = feature
            if norm_features:
                _, new_coef = measure_new_coef(
                    term, scalers, norm_features[index]
                )
            else:
                _, new_coef = measure_new_coef(term, scalers)
            all_terms.append((feature, new_coef))
    return all_terms


def write_simple_range_terms(file, scalers, features_limits):
    range_terms = range_constraints.copy()
    for constraint, terms in range_terms.items():
        terms = parse_complex_constraints(terms, features_limits, scalers)
        solver_operations.write_constraint(file, constraint, terms)


def define_ordinary_constraints(terms):
    return [(term["feature"], term["coef"]) for term in terms]


def parse_complex_constraints(
    terms, features_limits, scalers, range_min=None, range_max=None
):
    simple_constraint = []
    for term in terms:  # each term is a map
        if (
            len(term.keys()) == 2
            and "feature" in term.keys()
            and "coef" in term.keys()
        ):
            simple_constraint.append(tuple(term.values()))

        elif "limit" not in term.keys() and "operator" in term.keys():  # it
            # An operator - It has only one operator
            operator = tuple(term.values())

        elif "limit" in term.keys() and "operator" not in term.keys():
            limit = define_term_limit(
                term, features_limits, range_min, range_max
            )
            simple_constraint.append((term["feature"], limit))

        elif "limit" in term.keys() and "operator" in term.keys():
            if term["operator"] == "norm":
                method_operator = normalize_feature
            limit = define_term_limit(
                term, features_limits, range_min, range_max
            )
            if "norm_feature" in term.keys():
                feature = term["norm_feature"]
            else:
                feature = term["feature"]
            if "only_norm" in term.keys() and not term["only_norm"]:
                simple_constraint.append(
                    (
                        term["feature"],
                        term["coef"] * method_operator(scalers, feature, limit),
                    )
                )

            else:
                simple_constraint.append(
                    (term["coef"] * method_operator(scalers, feature, limit))
                )

        elif "start" in term.keys() and "end" in term.keys():
            simple_constraint.extend(define_range_terms(term, scalers))
    simple_constraint.append(operator)
    return simple_constraint


def define_term_limit(term, features_limits, range_min, range_max):
    if term["limit"] == "fmin":
        return range_min
    if term["limit"] == "fmax":
        return range_max
    if isinstance(term["limit"], float) or isinstance(term["limit"], int):
        return term["limit"]
    return features_limits[term["feature"]][term["limit"]]


def write_variable_constraints(
    file, features_limits, scalers, range_min, range_max
):
    """
    Write complex constraints with a function as feature's norm, lmin, lmax.
    """
    constraints = variable_constraints.copy()
    for constraint, terms in constraints.items():
        parsed_terms = parse_complex_constraints(
            terms, features_limits, scalers, range_min, range_max
        )
        solver_operations.write_constraint(file, constraint, parsed_terms)


def measure_new_coef(term, scalers, norm_feature=None):
    feature, new_coef = term["feature"], 1
    if "operator" in term.keys():
        if term["operator"] == "norm":
            if norm_feature:
                feature = norm_feature
            new_coef = normalize_feature(scalers, feature, term["limit"])
        elif term["operator"] == "scaler":
            if term["position"] == "range":
                new_coef = scalers[feature].data_range_[0]
            elif term["position"] == "min":
                new_coef = scalers[feature].data_min_[0]
            else:
                new_coef = scaler[feature].data_max_[0]
    new_coef *= term["coef"]
    return term, new_coef


def define_operator_term(terms):
    operations = ["LT", "LTE", "GTE", "GT", "E"]
    for index, term in enumerate(terms):
        condition_one = any(
            operation in term.values() for operation in operations
        )
        if condition_one and "operation" not in term.keys():
            operator = (term["operator"], term["coef"])
            terms.pop(index)
            return terms, operator
    raise ValueError(f"Operator not found. Input terms: {terms}")


def parse_range_complex_constraints(file, scalers, constraints):
    for constraint_name, terms in constraints.items():
        start, end = terms[0]["start"], terms[0]["end"] + 1
        # Define the constraint name
        constraint_name = constraint_name.format
        constraints_names = [
            constraint_name(value) for value in range(start, end)
        ]
        terms, operator = define_operator_term(terms)
        range_terms = list(
            map(lambda term: define_range_terms(term, scalers), terms)
        )
        composed_terms = [
            list(map(lambda term: term[term_index], range_terms))
            for term_index in range(0, len(range_terms[0]))
        ]
        for _constraint_name, _terms in zip(constraints_names, composed_terms):
            _terms = (*_terms, operator)
            solver_operations.write_constraint(file, _constraint_name, _terms)


def write_complex_constraints(file, scalers):
    constraints = complex_constraints.copy()
    for constraint, terms in constraints.items():
        new_terms = parse_type_complex_terms(constraint, terms, scalers)
        solver_operations.write_constraint(file, constraint, new_terms)


def parse_type_complex_terms(constraint_name, terms, scalers):
    operations = ["LT", "LTE", "GTE", "GT", "E"]
    new_terms, operator = [], []

    for term in terms:
        if "start" in term.keys() and "end" in term.keys():
            new_terms.extend(define_range_terms(term, scalers))

        elif any(operation in term.values() for operation in operations):
            operator = term.copy()

        # Complex factor after the operator
        elif "terms" in term.keys():
            right_terms = parse_type_complex_terms(
                constraint_name, term["terms"], scalers
            )

        elif "operator" in term.keys() and "position" in term.keys():
            _, new_coef = measure_new_coef(term, scalers)
            new_terms.append((term["feature"], new_coef))

        else:  # it is a static feature
            new_terms.append((term["feature"], term["coef"]))

    if isinstance(operator, dict):
        sum_coefs = sum([list(value)[1] for value in right_terms])
        operator = (operator["operator"], operator["coef"] * sum_coefs)
        new_terms.append(operator)

    return new_terms


def write_targets_limits(file, datasets, features_limits, us_sufix):
    """
    Write constraints to the targets of all models removing already defined
    constraints.
    """
    targets = list(map(lambda x: datasets[x].columns[-1], datasets))
    targets_write = list(filter(lambda x: x not in features_limits, targets))
    select_dataset = {
        target: list(filter(lambda x: target in datasets[x], datasets))
        for target in targets_write
    }
    for target in targets_write:
        values = datasets[select_dataset[target][0]][target]
        if us_sufix is "06":
            # NOTE: Hard-coded logic. Needs to be refactored and placed into
            #       a configuration file.
            if "COMP_MCOMP_PQ_L@08QU" in target:
                write_feature_constraints(
                    "compressao", file, min(values[values > 290]), max(values)
                )
                write_feature_constraints(
                    target, file, min(values[values > 290]), max(values)
                )
        elif target in TARGETS_IN_MODEL.keys():
            new_target = TARGETS_IN_MODEL[target]
            write_feature_constraints(
                new_target, file, min(values), max(values)
            )
        write_feature_constraints(target, file, min(values), max(values))


def write_equality_targets(file, scalers):
    for target, feature in TARGETS_IN_MODEL.items():
        constraint = []
        constraint_name = "equality_" + feature
        if feature in scalers:
            if scalers[feature].data_range_[0] > 0:
                constraint.append((target, -1))
                constraint.append((feature, scalers[feature].data_range_[0]))
                constraint.append(("E", -scalers[feature].data_min_[0]))
        else:
            constraint.append((target, -1))
            constraint.append((feature, 1))
            constraint.append(("E", 0))
        solver_operations.write_constraint(file, constraint_name, constraint)


def write_targets_limits_VG(file, datasets, features_limits):
    """
    Write constraints to the targets of all models
    removing those that are already defined
    """

    targets = list(map(lambda x: datasets[x].columns[-1], datasets))
    targets_write = list(filter(lambda x: x not in features_limits, targets))
    select_dataset = {
        target: list(filter(lambda x: target in datasets[x], datasets))
        for target in targets_write
    }

    for target in targets_write:
        values = (
            datasets[select_dataset[target][0]][target]
            .fillna(method="bfill")
            .fillna(method="ffill")
            .fillna(0)
        )
        if target == "0":
            continue
        elif target in "COMP_MCOMP_PQ_L@08QU":
            write_feature_constraints(target, file, 310, max(values))
            write_feature_constraints("compressao", file, 310, max(values))
        elif target in "QUIM_CFIX_PP_L@08PR":
            write_feature_constraints(
                target, file, min(values[values > 0]), 1.17
            )
            write_feature_constraints(
                "cfix", file, min(values[values > 0]), 1.17
            )

        elif target in "VgrLabRelGranuPQ-BN.2h":
            write_feature_constraints(
                target,
                file,
                min(values[values >= 0.5]),
                max(values[values <= 1.5]),
            )
            write_feature_constraints(
                "rel granulometrica",
                file,
                min(values[values >= 0.5]),
                max(values[values <= 1.5]),
            )

        elif target in TARGETS_IN_MODEL.keys():
            new_target = TARGETS_IN_MODEL[target]
            write_feature_constraints(
                new_target, file, min(values), max(values)
            )
            write_feature_constraints(target, file, min(values), max(values))
        else:
            write_feature_constraints(target, file, min(values), max(values))
