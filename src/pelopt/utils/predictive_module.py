import warnings

from pelopt.utils.logging_config import logger


warnings.filterwarnings("ignore")

header_row = 0


def write_row(sheet, row: int, values: list):
    for index, col in enumerate(values):
        sheet.write(row, index, col)


def write_header(sheet, header: list):
    write_row(sheet, header_row, header)


def retrieve_best_model(results, model, metric="mse", reverse=False):
    """Sort the models by it metrics and return the first (best one)"""
    return sorted(
        results[model], key=lambda x: x["metrics"][metric], reverse=reverse
    )[0]


def show_best_result(qualidade: str, best):
    logger.info(
        "{:25s} {:10s} {:6.2f}% {:6.2f}  {:6.2f}".format(
            qualidade,
            best["conf"],
            best["metrics"]["mape"],
            best["metrics"]["r2"],
            best["metrics"]["r"],
        )
    )


def tratando_dados(
    df_train,
    qualidade,
    col,
    rolling=10,
    fill_val=None,
    fill_method="bfill",
    restrict_to=None,
):
    # Preenchendo os valores nulos
    df_train[col] = df_train[col].interpolate().fillna(method=fill_method)

    # Aplicando média móvel
    if restrict_to is None or qualidade in restrict_to:
        df_train[col] = (
            df_train[col]
            .rolling(rolling)
            .mean()
            .fillna(fill_val, method=fill_method)
        )

    return df_train
