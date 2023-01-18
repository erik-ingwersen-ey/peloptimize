""" Operations applied over the data """
import json
import os
import re
import numpy as np
import os
import json
import re
import importlib
from sklearn.preprocessing import StandardScaler, MinMaxScaler


def normalize_feature(scalers, feature, norm_value):
    normalize_value = norm_value - scalers[feature].data_min_[0]

    if scalers[feature].data_range_[0] != 0:
        return normalize_value / scalers[feature].data_range_[0]

    else:
        return 0


def read_text_file(file):
    with open(file, "r") as file_data:
        return file_data.read()


def unnormalize_feature(scalers, feature, norm_value, operation="two feature"):
    scale_var = scalers[feature]

    if operation == "two feature":
        # old normalize var
        first_value = norm_value / scale_var.data_range_[0]
        second_value = (
            norm_value * scale_var.data_min_[0] / scale_var.data_range_[0]
        )
        return first_value, second_value

    else:
        return norm_value * scale_var.data_range_[0] + scale_var.data_min_[0]


def retrieve_dataset(feature, datasets):
    """
    Considering all datasets
    Returns the data where a column is present

    """
    return list(
        filter(lambda col: feature in datasets[col].columns, datasets.keys())
    )


def filter_list(list_one, list_two):
    """Returns the values in list one that are in list two"""
    return list(filter(lambda value: value in list_two, list_one))


def string_in_list(string, list_strings):
    if not string:
        return False

    for value in list_strings:
        if isinstance(value, str) and string.startswith(value):
            return True
    return False


def continue_regex(feature):
    if re.match(r"PESO1_I@08MO-BW-821I-[\d]*M1$", feature):
        return True
    return False


def read_json(file, encode=False):
    """Return the data within a json file"""
    if encode:
        with open(file, "rb") as json_file:
            return json.loads(json_file.read().decode("utf-8"))

    with open(file, "r") as json_file:
        return json.load(json_file)


def scaling_target_values(feature, scalers, lmin, lmax):
    lmin = (
        lmin * scalers[feature].data_range_[0] + scalers[feature].data_min_[0]
    )
    lmax = (
        lmax * scalers[feature].data_range_[0] + scalers[feature].data_min_[0]
    )

    return lmin, lmax, TARGETS_IN_MODEL[feature]


def replace_string_from_file(solver_path, range_min=None, range_max=None):
    if range_min and range_max:
        file_name = "restricoes-faixa-{}-{}.txt".format
        file = os.path.join(solver_path, file_name(range_min, range_max))
    else:
        file = solver_path

    # replacing tokens
    data = read_text_file(file).replace(".", ",")
    data = data.replace("; nan", "; 0,0")

    with open(file, "w") as constraint_file:

        constraint_file.write(data)


def build_new_plant_map(df_map, map_values, ignore=False):
    new_features = retrieve_from_map(df_map, list(map_values.keys()))
    new_dict = {}
    for key, feature in zip(map_values.keys(), new_features):
        if feature:
            new_dict[feature] = map_values[key]
        else:
            if not ignore:
                new_dict[key] = map_values[key]

    return new_dict


def retrieve_from_map(df_map, tags, dfs_read=None):
    flag = False

    if not isinstance(tags, list):
        tags = [tags]
        flag = True

    results = []

    for tag in tags:
        _tag = None
        if tag in df_map.keys():
            _tag = df_map[tag]
        elif dfs_read and any(tag in df for df in dfs):
            _tag = tag
        results.append(_tag)
    if results:
        if flag:
            return results[0]
        return results

    return None


def make_maps(df_map, subset, new_index, feature):
    """Return first dict and full dict."""
    df_dict = df_map.dropna(subset=[subset]).set_index(new_index)
    aux = df_dict.drop(columns=["Comentário"], errors="ignore")
    full_df = aux[~aux.index.duplicated()].to_dict("index")
    return df_dict[feature].to_dict(), full_df


# NOTE: essa função não FAZ O MENOR SENTIDO
def add_plant_new_keys(scalers):
    keys = list(scalers.keys())
    for key in keys:
        new_tag = scalers.get(key, None)
        if new_tag:
            scalers[new_tag] = scalers[key]
    return scalers


class Operations:
    @staticmethod
    def normalize_feature(scalers, feature, norm_value):

        normalize_value = norm_value - scalers[feature].data_min_[0]

        return normalize_value / scalers[feature].data_range_[0]

    @staticmethod
    def read_text_file(file):

        with open(file, "r", encoding="ISO-8859-1") as file_data:

            return file_data.read()

    @staticmethod
    def unnormalize_feature(
        scalers, feature, norm_value, operation="two feature"
    ):

        scale_var = scalers[feature]
        if operation == "two feature":
            # old normalize var
            first_value = norm_value / scale_var.data_range_[0]
            second_value = (
                norm_value * scale_var.data_min_[0] / scale_var.data_range_[0]
            )

            return first_value, second_value

        else:

            return (
                norm_value * scale_var.data_range_[0] + scale_var.data_min_[0]
            )

    @staticmethod
    def retrieve_dataset(feature, datasets):
        """
        Considering all datasets
        Returns a the data where a column is present

        """

        return list(
            filter(
                lambda col: feature in datasets[col].columns, datasets.keys()
            )
        )

    @staticmethod
    def filter_list(list_one, list_two):
        """Returns the values in list one that are in list two"""

        return list(filter(lambda value: value in list_two, list_one))

    @staticmethod
    def string_in_list(string, list_strings):

        if not string:
            return False

        for value in list_strings:
            if string.startswith(value):
                return True

        return False

    @staticmethod
    def continue_regex(feature):

        if re.match(r"PESO1_I@08MO-BW-821I-[\d]*M1$", feature):
            return True

        return False

    @staticmethod
    def read_json(file):
        """Return the data within a json file"""
        with open(file, "r") as json_file:

            return json.load(json_file)

    @staticmethod
    def read_json_dls(file_path, file_name):
        json_file = load_adls_json(file_path, file_name)

        return json_file

    @staticmethod
    def scaling_target_values(feature, scalers, lmin, lmax):

        lmin = (
            lmin * scalers[feature].data_range_[0]
            + scalers[feature].data_min_[0]
        )
        lmax = (
            lmax * scalers[feature].data_range_[0]
            + scalers[feature].data_min_[0]
        )

        return lmin, lmax, constants.TARGETS_IN_MODEL[feature]

    @staticmethod
    def replace_string_from_file(solver_path, range_min=None, range_max=None):

        if range_min and range_max:

            file_name = "restricoes-faixa-{}-{}.txt".format

            file = os.path.join(solver_path, file_name(range_min, range_max))

        else:

            file = solver_path

        # replacing tokens
        data = operations.read_text_file(file).replace(".", ",")

        with open(file, "w") as constraint_file:

            constraint_file.write(data)

    @staticmethod
    def define_real_scalers(datasets, df_map=None):

        all_columns = list(map(lambda x: list(datasets[x].columns), datasets))
        targets = list(map(lambda x: list(datasets[x].columns)[-1], datasets))
        all_columns = sum(all_columns, [])

        all_columns = list(
            filter(
                lambda x: "qtde" not in x
                and "NIVE1" not in x
                and "SOMA_FUNC" not in x,
                all_columns,
            )
        )

        scalers = {}

        for column in set(all_columns):
            if column.startswith("qtde") or column.startswith("SOMA FUNC"):
                continue

            all_df = list(
                filter(lambda x: column in datasets[x].columns, datasets)
            )
            scalers[column] = MinMaxScaler()

            if len(all_df) == 1:  # then this tag is in more than one dataset
                scalers[column].fit(datasets[all_df[0]][[column]])

            else:

                values = list(
                    map(lambda x: list(datasets[x][column].values), all_df)
                )
                values = sum(values, [])

                new_values = []
                for value in values:

                    if isinstance(value, np.float64) or isinstance(
                        value, np.int64
                    ):
                        new_values.append(value)
                    else:
                        new_values.extend(value)
                if "Consumo de Energia (base minério úmido) kWh/ton " in column:
                    new_values.append(0)
                new_values = np.array(new_values).reshape(-1, 1)
                scalers[column].fit(new_values)

        return scalers, new_values

    @staticmethod
    def define_integer_scalers(df_all, scalers):
        for col in df_all.columns:
            if col not in scalers and (
                col.startswith("qtde") or col.startswith("SOMA FUNC")
            ):
                scalers[col] = MinMaxScaler().fit(df_all[[col]])
        return scalers


operations = Operations()
