"""
Solver generic methods.
"""
import math

from pelopt.utils.logging_config import logger
from pelopt.utils.operations import (
    replace_string_from_file,
    unnormalize_feature,
)
from pelopt.utils.predictive_module import retrieve_best_model


class solver_operations:
    @staticmethod
    def write_constraint(
        file, constraint, terms, target=False, description=False, log_files=None
    ):
        """
        Write a set of constraints in a file:
        file: file name
        constraint: name of the restriction that will be added
        terms: terms that compose the constraint equation
        target: features that are target are treated in
        write_descriptive_contraints method
        """
        operators = ["E", "LTE", "LT", "GT", "GTE"]

        logger.info(
            f"write_constraint: {constraint} --- terms: {terms} --- target: "
            f"{target} --- description: {description}"
        )

        if description:
            target, coef, description = terms
            if description:
                logger.info(
                    "%s; %s; %s; %s", constraint, target, coef, description
                )

            else:
                print("{}; {}; {}".format(constraint, target, coef), file=file)

        elif target:
            target, new_tag, new_coef = terms
            print(
                "{}; {}; {}; {}".format(target, new_tag, new_coef, constraint),
                file=file,
            )
        else:
            if constraint:
                for index, term in enumerate(terms):
                    verify_int = isinstance(term, int)
                    verify_float = isinstance(term, float)

                    # it is not the last
                    verify_last = index != len(terms) - 1

                    if (
                        not isinstance(term, tuple)
                        and math.fabs(float(term)) <= 0.00001
                    ):
                        continue

                    if (verify_int or verify_float) and verify_last:
                        print("{}; {:.5f}".format(constraint, term), file=file)
                    else:
                        operation, value = term
                        if operation in operators:
                            logger.info(
                                f"operation: {constraint}, {operation}, "
                                f"{value}"
                            )
                            print(
                                "{}; {} {}".format(
                                    constraint, operation, value
                                ),
                                file=file,
                            )
                        else:
                            print(
                                "{}; {}; {}".format(
                                    constraint, operation, value
                                ),
                                file=file,
                            )

            else:
                feature, value = terms
                if math.fabs(float(value)) > 0.00001:
                    print("{}; {}".format(feature, value), file=file)

    @staticmethod
    def adjust_real_cost(real_cost, features, mult_coef=1, div_coef=3):
        """Adjust the real cost of some features"""
        for key in real_cost.keys():
            if key in features:
                real_cost[key] = (real_cost[key] * mult_coef) / div_coef

        return real_cost

    #     @staticmethod
    def retrieve_model_coeficients(model, models_results):
        """
        Return linear coefficients for each feature from the best model.
        """
        best_model = retrieve_best_model(models_results, model)

        # coefficient of each feature used in the model
        return zip(best_model["columns"], best_model["model"].coef_)

    @staticmethod
    def write_descriptive_contraints(
        solver_path,
        file,
        model_target,
        datasets,
        scalers,
        models_coeficients,
        features_coeficient,
        models_results,
        df_detailed=None,
        log_files=None,
    ):
        """
        Model target -> name of the selected model

        Some target constraints has a different treatment when compared to
        other features.

        This method treats the features.
        """
        denormalize_constant = 0
        logger.info(f"write_descriptive_contraints")
        print("write_descriptive_contraints")
        for tag, coef in features_coeficient:
            logger.info(f"TAG COEF: {tag}, {coef}")
            # if that tag repeats in the dataframe
            # nao usa nunca
            description = None
            if df_detailed:
                tag_count = df_detailed.columns.tolist().count(tag)
                description = ""
                if tag_count > 1:
                    description = df_detailed[tag].loc["Descrição"].iloc[0]
                elif tag_count == 1:
                    description = df_detailed[tag]["Descrição"]

            if tag in constants.define_targets(datasets):  # se for um target
                logger.info(f"if tag")
                new_coef, constant = unnormalize_feature(scalers, tag, coef)

                denormalize_constant -= constant
                models_coeficients[model_target][tag] = new_coef
                logger.info(f"tag, coef, new_coef: {tag}, {coef}, {new_coef}")

                solver_operations.write_constraint(
                    file,
                    model_target,
                    (tag, new_coef, description),
                    description=True,
                    log_files=log_files,
                )
            else:
                logger.info(f"else tag {coef}")
                solver_operations.write_constraint(
                    file,
                    model_target,
                    (tag, coef, description),
                    description=True,
                    log_files=log_files,
                )

        logger.info(f"write_simple_constraints")
        solver_operations.write_simple_constraints(
            solver_path,
            file,
            model_target,
            models_results,
            denormalize_constant,
            log_files=log_files,
        )

        return models_coeficients

    @staticmethod
    def write_simple_constraints(
        solver_path,
        file,
        model_target,
        models_results,
        unnormalize_constant,
        log_files=None,
    ):
        # Will return None when the target is rota_disco,
        # and when the tag limit is defined as None
        limit = (
            constants.LIMITS[model_target]
            if not model_target.startswith("rota_disco_")
            else None
        )

        best_conf = retrieve_best_model(models_results, model_target)

        if not limit:
            # o problema é aqui
            if "custo" in model_target:
                solver_operations.write_constraint(
                    file,
                    model_target,
                    [(model_target.replace("custo_", ""), -1)],
                    log_files=log_files,
                )

            else:
                solver_operations.write_constraint(
                    file,
                    model_target,
                    [(model_target, -1)],
                    log_files=log_files,
                )

            feat_target = None

            #  em us7, não existe mais energia_moinho1 e energia_moinho2 e
            #  essa parte do código estava quebrando a otimização
            if solver_path == "us6":
                if model_target.startswith("energia_moinho"):
                    feat_target = "FUNC1_D@08MO-MO-821I-{:02}M1".format(
                        int(model_target[-1])
                    )

            if model_target.startswith("rota_disco"):
                feat_target = "FUNC1_D@08PE-BD-840I-{:02}M1".format(
                    int(model_target[-1])
                )

            if feat_target:
                solver_operations.write_constraint(
                    file,
                    model_target,
                    [
                        (
                            feat_target,
                            best_conf["model"].intercept_
                            + unnormalize_constant,
                        )
                    ],
                    log_files=log_files,
                )

            else:
                solver_operations.write_constraint(
                    file,
                    None,
                    (
                        model_target,
                        best_conf["model"].intercept_ + unnormalize_constant,
                    ),
                    log_files=log_files,
                )

            solver_operations.write_constraint(
                file, model_target, [("E", "0")], log_files=log_files
            )

        else:
            operator, value = constants.LIMITS[model_target]

            solver_operations.write_constraint(
                file,
                None,
                (
                    model_target,
                    best_conf["model"].intercept_ + unnormalize_constant,
                ),
                log_files=log_files,
            )

            solver_operations.write_constraint(
                file, model_target, [(operator, value)], log_files=log_files
            )

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
    def scale_optimization_tags(scalers, real_cost, not_scale):
        """
        Escala os custos a partir dos scalers obtidos na otimização.

        Padrão é MinMaxScaler
        """
        opt_keys = list(
            filter(
                lambda col: col not in TARGETS_IN_MODEL.keys(), real_cost.keys()
            )
        )

        for key in opt_keys:
            # Essa versão de linha so é usada na US6, mas testada em todas
            # as outras
            if key.startswith(not_scale) or key not in scalers:
                continue
            # OBS.: o `data_range` é o max-min, mas do datasets
            real_cost[key] = scalers[key].data_range_[0] * real_cost[key]
        return real_cost

    @staticmethod
    def _features_in_scalers(scalers, features):
        scale_features = list(
            filter(lambda feature: feature in scalers, features)
        )
        return scale_features

    @staticmethod
    def save_new_format(df):
        path = "./resultado_otimizador/resultado_otimizador.csv"
        df = df.pivot("TAG", "faixa", "valor real")
        df.to_csv(path, sep=";", encoding="iso-8859-1")
        replace_string_from_file(path)

    @staticmethod
    def save_solver_results(df):
        format_tags = [
            "minimo",
            "maximo",
            "valor normalizado",
            "valor real",
            "custo",
        ]

        for tag in format_tags:
            df[tag] = df[tag].map("{:.5f}".format)

        columns_order = [
            "faixa",
            "TAG",
            "minimo",
            "maximo",
            "valor normalizado",
            "valor real",
            "custo",
        ]

        df = df.reindex(columns=columns_order)

        remove_tags = ["FUNC1_D@08MI-AM-832I-01M1"]

        df = df.loc[~df["TAG"].isin(remove_tags)]

        path = "./resultado_otimizador/resultado_otimizador-formato-antigo.csv"

        df.to_csv(path, sep=";", index=False, encoding="iso-8859-1")
        replace_string_from_file(path)
        solver_operations.save_new_format(df)
