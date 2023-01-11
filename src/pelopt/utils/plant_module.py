from pelopt.utils.operations import unnormalize_feature
from pelopt.utils.solver_operations import solver_operations


def write_antracito(file, scalers):
    constraint_name = "antracito_equal"
    constraint = [("VAZA1_Y@07HO-BP03", 1000), ("VAZA1_Y@07HO-BP04", 1000),
                  ("antracito", -scalers["VAZA2_I@07ES-BP"].data_range_[0]),
                  ("E", 0)]

    solver_operations.write_constraint(file, constraint_name, constraint)


# == Somente para a Usina 6 ====================================================
def write_taxa_alimentacao_bv(file, range_max):
    mass = 88.22
    func_token = "FUNC1_D@08FI-BV-827I-{:02}M1".format
    constraint_name = "taxa_alimentacao_bv_vs_producao"
    constraint = []

    for value in range(1, 9):
        if value % 5 == 0:
            constraint.append(
                ("FUNC1_D@08FI-BV-827I-{:02}RM1".format(value), mass)
            )
        else:
            constraint.append((func_token(value), mass))

    constraint.append(("GTE", range_max))

    solver_operations.write_constraint(file, constraint_name, constraint)


def write_taxa_alimentacao_filtros(file, range_max):
    mass = 88.22
    func_token = "FUNC1_D@08FI-FL-827I-{:02}".format
    constraint_name = "taxa_alimentacao_filtros_vs_producao"
    constraint = []

    for value in range(1, 9):
        if value % 5 == 0:
            constraint.append(
                ("FUNC1_D@08FI-FL-827I-{:02}R".format(value), mass)
            )
        else:
            constraint.append((func_token(value), mass))

    constraint.append(("GTE", range_max))

    solver_operations.write_constraint(file, constraint_name, constraint)


def write_temp_qugq_gt(file, scalers, datasets, production_query):
    for i in range(4, 13):
        constraint_name = "TEMP1_{:02}_maior_que_800".format(i)

        temp_token = "TEMP1_I@08QU-QU-855I-GQ{:02}".format(i)
        constraint_terms = []

        constraint_terms.append(
            (temp_token, scalers[temp_token].data_range_[0])
        )
        constraint_terms.append(("GTE", 800 - scalers[temp_token].data_min_[0]))

        solver_operations.write_constraint(
            file, constraint_name, constraint_terms
        )


def write_equality_potence(file, scalers, features_limits):
    feature = "POTE1_I@08FI-BV-827I-{:02}M1".format
    func_token = "FUNC1_D@08FI-BV-827I-{:02}M1".format

    features = []

    for index in range(1, 10):
        if index in [7]:
            continue
        elif index in [5]:
            features.append("POTE1_I@08FI-BV-827I-{:02}RM1".format(index))
        else:
            features.append(feature(index))

    func_feature = []
    for index in range(1, 9):
        if index in [5]:
            func_feature.append("FUNC1_D@08FI-BV-827I-05RM1")
        else:
            func_feature.append(func_token(index))

    index = 0

    for func_feature, pot_feature in zip(func_feature, features):
        constraint_name = "func_potence_equality_" + str(index + 1)

        values = [
            (func_feature, features_limits[pot_feature]["lmax"]),
            (pot_feature, -1),
            ("E", 0),
        ]

        solver_operations.write_constraint(file, constraint_name, values)

        index += 1


def write_rate_production_press(file, scalers):
    constraint = []

    constraint_name = "taxa_alimentacao_prensa_vs_producao"

    mass = 0.8762
    #     mass = 0.74

    rate_tag = "PESO1_I@08PR-BW-822I-01M1"

    coef = mass * scalers[rate_tag].data_range_[0]

    sum_coef = 0

    sum_coef -= mass * scalers[rate_tag].data_min_[0]

    constraint.append((rate_tag, coef))

    constraint.append(
        ("PROD_PQ_Y@08US", -scalers["PROD_PQ_Y@08US"].data_range_[0])
    )

    sum_coef += scalers["PROD_PQ_Y@08US"].data_min_[0]

    constraint.append(("E", sum_coef))

    solver_operations.write_constraint(file, constraint_name, constraint)


def write_disc_route_func(file, scalers, features_limits):
    func_token = "FUNC1_D@08PE-BD-840I-{:02}M1".format
    route_token = "ROTA1_I@08PE-BD-840I-{:02}M1".format

    constraint_name = "rota_func_equality_"

    tag_rota = "rota_disco_"
    for value in range(1, 7):
        constraint = []

        route_feature = route_token(value)

        unn_value = unnormalize_feature(
            scalers,
            "rota_disco_" + str(value),
            features_limits["rota_disco_" + str(value)]["lmax"],
            "one sided",
        )

        constraint.append((func_token(value), unn_value))
        constraint.append((tag_rota + str(value), -1))
        constraint.append(("E", 0))

        solver_operations.write_constraint(
            file, constraint_name + str(value), constraint
        )
