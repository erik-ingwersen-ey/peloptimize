from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from pelopt.config import TZ_SAO_PAULO
from pelopt.utils.logging_config import logger


def get_nulls_zeros(pandas_df: pd.DataFrame) -> pd.DataFrame:
    """Get dataframe with number of nulls and zeros on input dataframe columns.

    Parameters
    ----------
    pandas_df : pd.DataFrame
        Input dataframe to determine the number of nulls and zeros on
        each column.

    Returns
    -------
    pd.DataFrame
        Dataframe with the number of nulls and zeros on each column.
        Columns that have no values missing or equal to zero are not included.
        The resulting dataframe has two columns:

        - ``zero_count``: number of zeros on the column.
        - ``null_count``: number of nulls on the column .
    """
    nulls_zeros = (
        pandas_df.isnull()
        .sum()
        .to_frame("null_count")
        .join(
            pandas_df.apply(lambda col: (col == 0).sum()).to_frame("zero_count")
        )
        .loc[lambda xdf: (xdf["null_count"] > 0) | (xdf["zero_count"] > 0), :]
        .reset_index()
        .sort_values(["null_count", "zero_count"], ascending=False)
    )
    return nulls_zeros


def check_missing(pandas_df: pd.DataFrame):
    """
    Check if there are missing values on input dataframe.

    Function meant to check intermediate steps of the data preprocessing
    pipeline, defined inside the module:
    `preprocess <peloptimize/src/pelopt/usina/preprocess.py>`_ file.

    Parameters
    ----------
    pandas_df : pd.DataFrame
        Input dataframe to check for missing values.

    Notes
    -----
    This function is not part of the preprocessing pipeline. In other words,
    you can choose not to execute it, without affecting the rest of the
    preprocessing pipeline.
    """
    current_year_start = datetime.now(TZ_SAO_PAULO) - relativedelta(
        day=1, month=1
    )
    pandas_df_last_year = pandas_df[pandas_df.index < current_year_start]
    pandas_df_current_year = pandas_df[pandas_df.index >= current_year_start]
    teste_last_year = get_nulls_zeros(pandas_df_last_year)
    teste_current_year = get_nulls_zeros(pandas_df_current_year)
    if teste_last_year.empty and teste_current_year.empty:
        logger.info('No columns have values missing or equal to zero.')
    else:
        teste_df = pd.concat([teste_last_year, teste_current_year])
        total_missing = teste_df['null_count'].sum()
        total_zeroes = teste_df['zero_count'].sum()
        total_errors = total_missing + total_zeroes
        logger.warning('Found %s columns with errors. Missing count: %s - '
                       'Zeroes count: %s - Total: %s', teste_df.shape[0],
                       total_missing, total_zeroes, total_errors)
