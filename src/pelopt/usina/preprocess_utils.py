from typing import List

import numpy as np
import pandas as pd

from pelopt.utils.logging_config import logger


def drop_duplicate_tags(tags_list: List[str]) -> List[str]:
    """
    Drop duplicate tags from a list of tags.

    When duplicate tags are found, function logs a warning message, informing
    the number of duplicate tags that were found.

    Parameters
    ----------
    tags_list : List[str]
        List of tag names.

    Returns
    -------
    List[str]
        List of tags without duplicate names.
    """
    number_of_tags = len(tags_list)
    tags_list = list(set(tags_list))
    new_number_of_tags = len(tags_list)
    if new_number_of_tags < number_of_tags:
        number_of_duplicated_tags = number_of_tags - new_number_of_tags
        logger.warning(
            "Duplicate tags found. Removed %s (%s) duplicate tags.",
            number_of_duplicated_tags,
            f"{number_of_duplicated_tags / number_of_tags:.2%}",
        )
    return tags_list


def create_plant_production_bands(
    lower_bound: int,
    upper_bound: int,
    increment: int = 50,
) -> pd.DataFrame:
    """Create a pandas DataFrame with the production bands for a plant.

    Parameters
    ----------
    lower_bound : int
        Lower bound of the production bands. Must be an integer greater than 0.
    upper_bound : int
        Upper bound of the production bands. Must be an integer
        greater than :param:`lower_bound`.
    increment : int, default 50
        Increment between production bands.

    Returns
    -------
    pd.DataFrame
        DataFrame with the production bands for a plant.
        The resulting DataFrame has two columns:

        - ``faixaMin``: lower bound of the production band.
        - ``faixaMax``: upper bound of the production band.

    Raises
    ------
    ValueError
        If :param:`lower_bound` is not an integer greater than 0.
        If :param:`upper_bound` is not an integer greater
        than :param:`lower_bound`.

    Examples
    --------
    >>> create_plant_production_bands(700, 1000)
       faixaMin  faixaMax
    0       700       750
    1       750       800
    2       800       850
    3       850       900
    4       900       950
    5       950      1000
    """
    if not 0 < lower_bound < upper_bound:
        raise ValueError(
            "`lower_bound` must be an integer greater than 0 and "
            "`upper_bound` must be an integer greater than `lower_bound`."
            f"Got `lower_bound` = {lower_bound} and "
            f"`upper_bound` = {upper_bound}."
        )
    return pd.DataFrame(
        {
            "faixaMin": list(range(lower_bound, upper_bound, increment)),
            "faixaMax": list(
                range(
                    lower_bound + increment, upper_bound + increment, increment
                )
            ),
        }
    )


def fix_vacuum_pressure_columns(
    pandas_df: pd.DataFrame,
    plant_number: str,
) -> pd.DataFrame:
    """Fix potential errors with the vacuum pressure sensor data.

    Parameters
    ----------
    pandas_df : pd.DataFrame
        DataFrame with the vacuum pressure sensor data.
    plant_number : str
        The number of the pelleting plant that the data belongs to.

    Returns
    -------
    pd.DataFrame
        DataFrame without bad vacuum pressure sensor data for the specified
    """
    vacuum_pressure_cols = [
        col
        for col in pandas_df.columns
        if col.startswith(f"PRES1_I@{plant_number}FI-FL-827I-")
    ]
    for t in pandas_df.loc[:, vacuum_pressure_cols]:
        pandas_df.loc[pandas_df[t] > -0.8, t] = np.nan
    return pandas_df.drop_duplicates(keep=False).replace(
        [-np.inf, np.inf], np.nan
    )


def fix_disk_rotations(
    pandas_df: pd.DataFrame, plant_number: str
) -> pd.DataFrame:
    """Fix potential errors with the disk rotations sensor data.

    Disk rotation sensors use Tags that follow
    the pattern: ``"ROTA1_I@<PLANT-NUMBER>FI-FL-827I-"``.

    Parameters
    ----------
    pandas_df : pd.DataFrame
        DataFrame with the disk rotations sensor data.
    plant_number : str
        The number of the pelleting plant that the data belongs to.
        This identifier is used to identify the Tags related to the disk
        rotations of the plant.

    Returns
    -------
    pd.DataFrame
        DataFrame without bad disk rotations sensor data for the specified
        plant.
    """
    plant_number = str(plant_number)
    plant_number = f'0{plant_number}' if len(plant_number) == 1 else plant_number
    for t in pandas_df.loc[
        :,
        [
            col
            for col in pandas_df.columns
            if f"ROTA1_I@{plant_number}FI-FL-827I-" in col
        ],
    ]:
        pandas_df.loc[((pandas_df[t] < 0.8) | (pandas_df[t] > 1)), t] = np.nan
    return (
        pandas_df.fillna(method="bfill")
        .fillna(method="ffill")
        .fillna(method="bfill")
        .fillna(0)
    )
