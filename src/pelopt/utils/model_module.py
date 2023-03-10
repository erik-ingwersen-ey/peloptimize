from __future__ import annotations

import itertools
import random
import warnings
from pathlib import Path
from typing import Callable, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from IPython.display import HTML
from pyod.models.abod import ABOD
from pyod.models.cblof import CBLOF
from pyod.models.feature_bagging import FeatureBagging
from pyod.models.hbos import HBOS
from pyod.models.iforest import IForest
from pyod.models.knn import KNN
from pyod.models.lof import LOF
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import KFold
from wcmatch import glob

from pelopt.utils.logging_config import logger


def data_quality(df: pd.DataFrame) -> pd.DataFrame:
    """Verify data quality.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to verify.

    Returns
    -------
    pd.DataFrame
        Dataframe with data quality information. The columns are:

        - ``Atributo``: column name.
        - ``NaN``: number of ``NaN`` values.
        - ``+inf``: number of ``+inf`` values.
        - ``-inf``: number of ``-inf`` values.

    """
    logger.info("Row count: %s - Column count: %s", *df.shape)
    return pd.DataFrame(
        [
            [
                col,                        # Nome da coluna
                df[i].isna().sum(),         # Quantidade de NaN
                (df[i] == np.inf).sum(),    # Quantidade de +inf
                (df[i] == -np.inf).sum(),   # Quantidade de -inf
            ]
            for col in df.columns
        ],
        columns=["Atributo", "NaN", "+inf", "-inf"],
    ).sort_values(by=["NaN", "+inf", "-inf"], ascending=False)


def params_to_str(params: dict, exclude: List[str] | str | None = None) -> str:
    """Convert a dictionary to string.

    Function

    Parameters
    ----------
    params : dict
        Dictionary to convert.
    exclude: List[str] | str | None, optional
        Keys to exclude from the string conversion.
        When ``None`` (default), excludes keys: ``"random_state"``, ``"seed"``.

    Returns
    -------
    str
        Dictionary converted to string. Keys are separated by ``;``.
    """
    if exclude is None:
        exclude = {"random_state", "seed"}
    elif isinstance(exclude, str):
        exclude = {exclude}

    return ";".join(
        f"{format_key(key)}={format_value(value)}"
        for key, value in params.items()
        if key not in exclude
    )


def format_key(key):
    if "_" in key:
        return "".join(s[0].upper() for s in key.split("_"))
    return key[:4]


def format_value(value):
    if value:
        return "Y"
    if not value:
        return "N"
    return "-" if value is None else value


def hide_toggle(for_next=False):
    this_cell = """$('div.cell.code_cell.rendered.selected')"""
    next_cell = f"{this_cell}.next()"
    toggle_text = "Toggle show/hide"  # Text shown on toggle link
    target_cell = this_cell  # Target cell to control with toggle
    # A bit of JS to permanently hide code
    # in current cell (only when toggling next cell)
    js_hide_current = ""

    if for_next:
        target_cell = next_cell
        toggle_text += " next cell"
        js_hide_current = f'{this_cell}.find("div.input").hide();'
    js_f_name = f"code_toggle_{random.randint(1, 2 ** 64)}"
    html = """
        <script>
            function {f_name}() {{
                {cell_selector}.find('div.input').toggle();
            }}
            {js_hide_current}
        </script>
        <a href="javascript:{f_name}()">{toggle_text}</a>
    """.format(
        f_name=js_f_name,
        cell_selector=target_cell,
        js_hide_current=js_hide_current,
        toggle_text=toggle_text,
    )
    return HTML(html)


def load_df(csv_names: dict, callback=None, **reader_kwargs):
    datasets = {}
    for key, value in csv_names.items():  # itera????o sobre todos os dfs
        value = Path(value)
        if value.is_dir():
            filename = {
                Path(fname).with_suffix("").name: value.joinpath(fname)
                for fname in glob.glob(
                    "*.{csv,txt,xlsx,json}", flags=glob.BRACE, root_dir=value
                )
            }
            res_dict = load_df(filename, callback)
            logger.info("Fim do, %s, (dir)", value)
            datasets.update(res_dict)
        else:
            if value.suffix in [".csv", ".txt"]:
                dataset = pd.read_csv(value, **reader_kwargs)
                if dataset.shape[1] <= 1:
                    reader_kwargs.pop("sep", None)
                    _dataset = pd.read_csv(value, sep=None, **reader_kwargs)
                    if _dataset.shape[1] > 1:
                        dataset = _dataset
                datasets[key] = dataset
            elif value.suffix == ".xlsx":
                datasets[key] = pd.read_excel(value, **reader_kwargs)
            elif value.suffix == ".json":
                datasets[key] = pd.read_json(value, **reader_kwargs)
            if isinstance(callback, Callable):
                datasets[key] = callback(datasets[key])
    return datasets


def show_coef(results):
    # Coeficientes
    best = sorted(results, key=lambda r: -r["metrics"]["r2_train"])[0]
    logger.info(best["conf"], best["params"], best["metrics"])
    for a, b in sorted(
        list(zip(best["columns"].tolist(), best["model"].coef_)),
        key=lambda e: -abs(e[1]),
    ):
        logger.info("%-10.4s  %s", str(b), str(a))


def xgb_imprime_coef(self):
    return self.get_booster().get_score(importance_type="gain")


def ridge_imprime_coef(results=None):
    best = sorted(results, key=lambda r: -r["metrics"]["r2_train"])[0]
    return dict(
        sorted(
            list(zip(best["columns"].tolist(), best["model"].coef_)),
            key=lambda e: -abs(e[1]),
        )
    )


def param_combination(model, **kwargs):
    model_params = {
        k: (v if isinstance(v, list) else [v])
        for k, v in model["params"].items()
    }
    model_params["random_state"] = [0]
    keys = sorted(model_params.keys())
    return [
        dict(zip(keys, prod))
        for prod in itertools.product(*(model_params[k] for k in keys))
    ]


def kfold_validation(model, df_train, df_target, **kwargs):
    predicted_g = []
    ys = []
    yhats = []
    metrics = []
    indexes = []

    kfold = KFold(10, shuffle=True, random_state=0)
    for train_ix, test_ix in kfold.split(df_train):
        train, trainy = df_train.iloc[train_ix], df_target.iloc[train_ix]
        test, testy = df_train.iloc[test_ix], df_target.iloc[test_ix]
        model.fit(train, trainy)
        p_train = model.predict(train)
        predicted = model.predict(test)
        predicted_g = np.concatenate((predicted_g, predicted))
        mse = mean_squared_error(testy, predicted)
        mape = np.mean(np.abs(testy - predicted) / np.abs(testy)) * 100
        r2 = r2_score(testy, predicted)
        r = pd.DataFrame(predicted)[0].corr(pd.DataFrame(testy.values)[0])
        r2_train = r2_score(trainy, p_train)
        n, p = train.shape
        r2_train_adj = 1 - (1 - r2_train) * (n - 1) / (n - p - 1)
        metrics.append((mse, mape, r2, r, r2_train, r2_train_adj))
        indexes.extend(testy.index)
        ys.extend(testy)
        yhats.extend(predicted)
    mse = np.mean([metric[0] for metric in metrics])
    mape = np.mean([metric[1] for metric in metrics])
    r2 = np.mean([metric[2] for metric in metrics])
    r = np.mean([metric[3] for metric in metrics])
    r2_train = np.mean([metric[4] for metric in metrics])
    r2_train_adj = np.mean([metric[5] for metric in metrics])
    return indexes, ys, yhats, (mse, mape, r2, r, r2_train, r2_train_adj)


def plot_charts(
    model_conf,
    df_target,
    predicted,
    qualidade,
    params,
    indexes,
    ys,
    yhats,
    metrics,
    solver_path,
    **kwargs,
):
    mse, mape, r2, r, r2_train, r2_train_adj = metrics

    graph_title = qualidade + "_" + model_conf + params_to_str(params)

    fpath = Path(kwargs.get("fpath", f"/dbfs/tmp/{solver_path}/Graficos"))
    fpath.mkdir(parents=True, exist_ok=True)
    _fpath = fpath.joinpath("Line")
    _fpath.mkdir(parents=True, exist_ok=True)
    graph_name_1 = _fpath.joinpath(f"{graph_title}.png")

    _fpath = fpath.joinpath("Scatter")
    _fpath.mkdir(parents=True, exist_ok=True)
    graph_name_2 = _fpath.joinpath(f"{graph_title}.png")

    dg = pd.DataFrame(
        {"Processo": indexes, "Real": ys, "Predicted": yhats}
    ).set_index("Processo")

    dg.plot.line(figsize=(20, 10), fontsize=10)
    plt.title(graph_title, fontsize=20)
    plt.xlabel("DATA", fontsize=15)
    plt.ylabel(qualidade.upper(), fontsize=20)
    plt.savefig(graph_name_1)
    plt.close()

    dg.plot.scatter(x="Real", y="Predicted", c="DarkBlue", figsize=(15, 15))
    plt.title(graph_title, fontsize=20)
    plt.axis("equal")
    plt.xlabel("REAL", fontsize=15)
    plt.ylabel("PREDITO", fontsize=15)
    plt.savefig(graph_name_2)
    plt.close()


def apply_model(
    model,
    model_name,
    model_conf,
    df_train,
    df_target,
    solver_path,
    selfTrain=True,
    parameter_combination=param_combination,
    validation=kfold_validation,
    plot_charts=plot_charts,
    **kwargs,
):
    logger.info(f"###  + {model_conf}")

    # Chaves para cada fun????o
    param_comb = kwargs.get("param_combination", None)
    param_valid = kwargs.get("param_validation", None)
    param_plot = kwargs.get("param_plotting", None)

    results = []
    comb_params = parameter_combination(
        model,
        # model_conf,
        kwparams=param_comb,
    )
    for params in comb_params:
        method = model["model"](**params)

        # Applying the k-fold fit model
        try:
            indexes, ys, yhats, metrics = validation(
                method, df_train, df_target, kwparams=param_valid
            )
        except Exception as e:
            return e

        # Taking the metrics
        mse, mape, r2, r, r2_train, r2_train_adj = metrics

        # Retrain model with all dataset
        if selfTrain:
            method.fit(df_train, df_target)
            predicted = method.predict(df_train)
        else:
            predicted = None

        # plotting charts
        plot_charts(
            model_conf,
            df_target,
            predicted,
            model_name,
            params,
            indexes,
            ys,
            yhats,
            metrics,
            solver_path,
            kwparams=param_plot,
            fpath=f"./tmp/{solver_path}/Graficos",
        )

        # Appending the best results
        results.append(
            {
                "conf": model_conf,
                "metrics": {
                    "mse": mse,
                    "mape": mape,
                    "r2": r2,
                    "r": r,
                    "r2_train": r2_train,
                    "r2_train_adj": r2_train_adj,
                },
                "model": method,
                "columns": df_train.columns,
                "params": params,
                "indexes": indexes,
                "ys": ys,
                "yhats": yhats,
                "predicted": predicted,
            }
        )
        result_str = (
            "{:.<20} - MSE: {:6.3f} - MAPE: {:6.3f}% - r2: {:6.3f} - "
            "r: {:6.3f} - r2_train: {:6.3f} - r2_train_adj: {:6.3f}"
        )
        logger.info(
            result_str.format(
                params_to_str(params), mse, mape, r2, r, r2_train, r2_train_adj
            )
        )

    return results


# TODO: Usar a coluna de threshold para aplicar os limites?
def miss_test(df_limits, df_target, df_predicted, threshold=0.1):
    # Definindo colunas para o target e o predito
    df_limits["target"] = df_target
    df_limits["predic"] = df_predicted

    # Adicionando o threshold para o min e max
    df_limits["min"] = df_limits["min"] - threshold
    df_limits["max"] = df_limits["max"] + threshold

    # Aplicando o teste
    miss = []
    total = 0.0

    for indx, row in df_limits.iterrows():
        if (row["target"] >= row["min"]) and (row["target"] <= row["max"]):
            miss.append(0)
        elif (row["target"] < row["min"]) and (row["predic"] < row["min"]):
            miss.append(0)
        elif (row["target"] > row["max"]) and (row["predic"] > row["max"]):
            miss.append(0)
        else:
            miss.append(1)

        if (row["target"] < row["min"]) or (row["target"] > row["max"]):
            total += 1.0

    df_limits["miss"] = miss

    logger.info("Predicted Miss: %s", df_limits.miss.sum())
    logger.info("Target Miss: %s", total)
    # logger.info(f"Porcentagem: {100 * df_limits.miss.sum() / df_limits.shape[
    # 0]})
    logger.info(f"Porcentagem: {100 * df_limits.miss.sum() / total}")

    return total, df_limits


def multi_model_outlier_classification(df, outliers_fraction=0.15, n_jobs=-1):
    random_state = np.random.RandomState(42)
    outliers_fraction = 0.15

    # Define 7 outlier detection methods to compare
    classifiers = {
        "Angle-based Outlier Detector (ABOD)": ABOD(
            contamination=outliers_fraction,
        ),
        "Cluster-based Local Outlier Factor (CBLOF)": CBLOF(
            contamination=outliers_fraction,
            check_estimator=False,
            random_state=random_state,
            n_jobs=n_jobs,
        ),
        "Feature Bagging": FeatureBagging(
            LOF(n_neighbors=35),
            contamination=outliers_fraction,
            check_estimator=False,
            random_state=random_state,
            n_jobs=n_jobs,
        ),
        "Histogram-base Outlier Detection (HBOS)": HBOS(
            contamination=outliers_fraction
        ),
        "Isolation Forest": IForest(
            contamination=outliers_fraction,
            random_state=random_state,
            n_jobs=n_jobs,
        ),
        "K Nearest Neighbors (KNN)": KNN(
            contamination=outliers_fraction, n_jobs=n_jobs
        ),
        "Average KNN": KNN(
            method="mean", contamination=outliers_fraction, n_jobs=n_jobs
        ),
    }
    dfx = df.copy()
    dfx["outlier"] = 0
    for clf_name, clf in classifiers.items():
        logger.debug("Running: %s", clf_name)
        clf.fit(dfx)
        y_pred = clf.predict(dfx)
        dfx["outlier"] += y_pred.tolist()
        logger.debug("%s Ok!", clf_name)

    limite_minimo_outlier = 0
    dfx.outlier = np.where(dfx.outlier > limite_minimo_outlier, 1, 0)
    return dfx[dfx["outlier"] == 0].drop(columns=["outlier"])
