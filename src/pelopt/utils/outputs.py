""" Write output files. """
import os
import re

import pandas as pd

from pelopt.utils.logging_config import logger
from pelopt.utils.operations import (
    read_json,
    replace_string_from_file,
    retrieve_from_map,
    unnormalize_feature,
)
from pelopt.utils.solver_operations import solver_operations


def write_objective_function_coef(solver_path, scalers, df_map=None):
    logger.info("write_objective_function_coef")
    real_cost = read_json(f"{solver_path}/custo_real.json", True)

    # Itera em grupo de custos: Consumo de Energia
    if "us6" in solver_path or "usVG" in solver_path:
        upper_range = 3
    elif "us7" in solver_path:
        upper_range = 4
    tokens = solver_operations.define_range_constraints(
        "Consumo de Energia (base minério úmido) kWh/ton {}", 1, upper_range
    )
    real_cost = solver_operations.adjust_real_cost(real_cost, tokens)
    # Itera em grupo de custos: corpo_moedor_especifico
    if "usVG" in solver_path:
        upper_range = 2
    tokens = solver_operations.define_range_constraints(
        "corpo_moedor_especifico_{}", 1, upper_range
    )
    real_cost = solver_operations.adjust_real_cost(real_cost, tokens, 1000)

    # Reescala valores (desnormaliza), ignora 'POTE1_I@08FI-BV',
    # mas para que fazer isso? Provavelmente porque as quantidades estão
    # normalizadas (minmaxscaler), então precisa compensar na hora que vai
    # otimizar.
    opt_costs = solver_operations.scale_optimization_tags(
        scalers, real_cost, "POTE1_I@08FI-BV"
    )
    obj_coefs = pd.DataFrame.from_dict(
        opt_costs, orient="index", columns=["Custo"]
    )
    obj_coefs.index.name = "TAG"

    if df_map:
        new_features = retrieve_from_map(df_map, list(obj_coefs.index))
        for index, feature in zip(obj_coefs.index, new_features):
            if feature:
                obj_coefs = obj_coefs.rename(index={index: feature})
    obj_coefs = obj_coefs[obj_coefs.index.isin(OBJ_FUNC_COEF)]
    obj_coefs.to_csv(
        os.path.join(solver_path, "costs.csv"), decimal=",", sep=";"
    )


def parse_variables_name(variables_name):
    return list(
        map(lambda var: re.search(r"\((.*)\)", var).group(1), variables_name)
    )[0]


def parse_coefficients(coefficients):
    return list(map(lambda coef: float(coef.replace(",", ".")), coefficients))[
        0
    ]


def save_new_format(df):
    path = "./resultado_otimizador/resultado_otimizador.csv"

    df = df.pivot("TAG", "faixa", "valor real")
    df.to_csv(path, sep=";", encoding="iso-8859-1")
    replace_string_from_file(path)


def save_results(path, df, remove_tags=None):
    format_tags = [
        "minimo",
        "maximo",
        "valor normalizado",
        "valor real",
        "custo",
    ]
    columns_order = [
        "faixa",
        "TAG",
        "minimo",
        "maximo",
        "valor normalizado",
        "valor real",
        "custo",
    ]
    for tag in format_tags:
        df[tag] = df[tag].map("{:.5f}".format)
    df = df.reindex(columns=columns_order)
    if remove_tags:
        df = df.loc[~df["TAG"].isin(remove_tags)]
    df.to_csv(path, encoding="iso-8859-1")


def apply_normalization(var_df, scalers):
    columns = [" Value", " LB", " UB"]
    var_df[" ObjCoeff"] = var_df[" Value"]
    for column in columns:
        var_df[column] = unnormalize_feature(
            scalers, var_df[" VariableName"], var_df[column], "one_side"
        )
    return var_df


def _rename_columns(df):
    columns = {
        " VariableName": "TAG",
        " LB": "minimo",
        " UB": "maximo",
        " ObjCoeff": "valor normalizado",
        " Value": "valor real",
        "range": "faixa",
    }
    for var_df in df:
        if isinstance(var_df, pd.DataFrame):
            var_df.rename(columns=columns, inplace=True)
        else:
            for key, value in columns.items():
                var_df[value] = var_df.pop(key)
    return df
