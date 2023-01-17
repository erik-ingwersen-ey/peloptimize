"""
This module contains the configuration for the ``pelopt`` package.

Configurations
--------------
- ``USINAS``: dictionary with information related to each pelletizing plant.
- ``DEFAULT_TINI``: default start date to retrieve sensor data from.
- ``DEFAULT_TFIM``: default end date to retrieve sensor data from.

"""
from datetime import datetime

import pytz
from dateutil.relativedelta import relativedelta


TZ_SAO_PAULO = pytz.timezone("America/Sao_Paulo")
"""Fuso horário para manipulação de datas."""

TESTING_PREPROCESSING = False
TESTING_PARAMS = {
    "selected_time": datetime(2022, 7, 22, 0, 0, 0, tzinfo=TZ_SAO_PAULO)
}

USINAS = {
    8: {
        "numero_usina": 8,
        "numero_usina_str": "08",
        "solver_path_usina": "us8",
        "usina_path": "US8",
        "usina_path_str": "US8",
        "faixa_min": 700,
        "faixa_max": 1000,
    },
}
DEFAULT_TFIM = datetime.now(TZ_SAO_PAULO).replace(
    microsecond=0, second=0, minute=0
)
DEFAULT_TINI = (DEFAULT_TFIM - relativedelta(days=547)).replace(
    microsecond=0, second=0, minute=0
)
DEFAULT_TINI_STR = DEFAULT_TINI.strftime("%Y-%m-%d %H:%M:%S")
DEFAULT_TFIM_STR = DEFAULT_TFIM.strftime("%Y-%m-%d %H:%M:%S")
