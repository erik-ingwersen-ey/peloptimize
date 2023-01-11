from __future__ import annotations
from typing import List

import dbutils
import numpy as np
import pandas as pd

from pelopt.config import DEFAULT_TFIM, DEFAULT_TINI_STR, USINAS
from pelopt.tags import tagpims, tagOTM, taglab, tag_calc
from pelopt.usina.preprocess_utils import drop_duplicate_tags
from pelopt.utils.logging_config import logger
from pelopt.utils.sql_module import sqlModule


NUMERO_USINA = 8
usina = USINAS[NUMERO_USINA]
tagpims = drop_duplicate_tags(tagpims + tagOTM + taglab + tag_calc)

# tagdl = TAGs Datalake
tagdl = sqlModule(dbutils).tagPIMStoDataLake(tagpims)
taglist = pd.DataFrame({"tagdl": tagdl, "tagpims": tagpims})

df_sql = sqlModule(dbutils).getFVarParam(
    NUMERO_USINA,
    DEFAULT_TINI_STR,
    DEFAULT_TFIM,
    taglist,
    param=usina["usina_path"],
)
df_sql_grouped = df_sql.groupby("variavel", as_index=False).count()

df_sql = (
    df_sql
    # Remove registros duplicados, se existirem
    .drop_duplicates(subset=["data", "variavel"], keep="first")
    # Transforma cada valor da coluna "variavel" em uma coluna, com "valor"
    # como valor.
    .pivot(index="data", columns="variavel", values="valor")
)


