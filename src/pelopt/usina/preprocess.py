from __future__ import annotations
from typing import List

from datetime import datetime
import dbutils
import numpy as np
import pandas as pd

from pelopt.config import DEFAULT_TFIM, DEFAULT_TINI, DEFAULT_TINI_STR, USINAS
from pelopt.dataprep.datatools import check_missing
from pelopt.tags import tagpims, tagOTM, taglab, tag_calc
from pelopt.usina.calc_tags import get_calculated_tags
from pelopt.usina.preprocess_utils import (
    drop_duplicate_tags,
    fix_disk_rotations,
    fix_vacuum_pressure_columns,
)
from pelopt.utils.logging_config import logger
from pelopt.utils.sql_module import sqlModule


def preprocess_data(
    numero_usina: int,
    tagpims: List[str],
    tagOTM: List[str],
    taglab: List[str],
    tag_calc: List[str],
    tfim: datetime | None = None,
    tini: datetime | None = None,
):

    if tfim is None:
        tfim = DEFAULT_TFIM
    if tini is None:
        tini = DEFAULT_TINI
    usina = USINAS[numero_usina]
    tagpims = drop_duplicate_tags(tagpims + tagOTM + taglab + tag_calc)

    # tagdl = TAGs Datalake
    tagdl = sqlModule(dbutils).tagPIMStoDataLake(tagpims)
    taglist = pd.DataFrame({"tagdl": tagdl, "tagpims": tagpims})

    df_sql = sqlModule(dbutils).getFVarParam(
        numero_usina,
        DEFAULT_TINI_STR,
        tfim,
        taglist,
        param=usina["usina_path"],
    )
    df_sql_grouped = df_sql.groupby("variavel", as_index=False).count()

    df_sql = (
        df_sql
        # Remove registros duplicados, se existirem
        .drop_duplicates(subset=["data", "variavel"], keep="first")
        # Transforma cada tag em uma coluna, com "valor" como valor.
        .pivot(index="data", columns="variavel", values="valor")
    )

    check_missing(df_sql)

    df_sql = sqlModule(dbutils, spark).getTAGsDelay(
        df_sql, tini, tfim, tagpims, taglist
    )
    df_sql_raw = df_sql.copy()
    df_sql = df_sql_raw.copy().loc[lambda xdf: xdf["PROD_PQ_Y@08US"] > 0, :]

    df_sql = (
        fix_vacuum_pressure_columns(df_sql, f"{numero_usina:02d}")
        .drop_duplicates(keep=False)
        # .replace([-np.inf, np.inf], np.nan)
        .pipe(fix_disk_rotations, numero_usina)
    )
    df_sql = get_calculated_tags(numero_usina, df_sql, tagpims)

    var, k, mult = (
        f"VAZA1_I@0{numero_usina}QU-ST-855I-01",
        f"11 - TRATAMENTO TERMICO - SPSS - US{numero_usina}",
        1,
    )
    prodpq = df_sql[f"PROD_PQ_Y@0{numero_usina}US"].copy()
    p = prodpq > 0
    try:
        df_sql = df_sql[p]  # Removing production equal to 0
    except Exception as err:
        logger.exception(err)
    df_sql[var] = df_sql[var] / df_sql[f"PROD_PQ_Y@0{numero_usina}US"][p] * mult


if __name__ == "__main__":

    NUMERO_USINA = 8
    preprocess_data(
        NUMERO_USINA,
        tagpims,
        tagOTM,
        taglab,
        tag_calc,
    )
