"""
Module with instructions on how to calculate additional Tags from base Tags.
"""
import math
from typing import List

import numpy as np
import pandas as pd


def get_calculated_tags(
    plant_number: int, df_sql: pd.DataFrame, tags_needed: List[str]
) -> pd.DataFrame:
    """
    Calculate tags obtainable by combining other tags and algebraic expressions.

    Parameters
    ----------
    plant_number : int
        The plant number to calculate the tags for.
    df_sql : pd.DataFrame
        Table with original Tags needed to obtain the calculated tags.
    tags_needed : List[str]
        List of tags to be calculated. This list avoids trying to calculate
        tags that are not needed.

    Returns
    -------
    pd.DataFrame
        Table with the required calculated Tags.
    """
    plant_number_long = f"{int(plant_number):02d}"
    plant_number = plant_number_long[-1]

    if f"CONS ESP VENT TOTAL - US{plant_number}" in tags_needed:
        df_sql[f"CONS ESP VENT TOTAL - US{plant_number}"] = df_sql[
            f"CONS1_Y@{plant_number_long}QU-VENT"
        ]

    if f"PERMEABILIDADE CV27 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV27 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-27"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if "Vel Periferica R Fixo [m/s]" in tags_needed:
        df_sql["Vel Periferica R Fixo [m/s]"] = (
            math.pi
            * df_sql[f"ROTA1_I@{plant_number_long}PR-RP-822I-01M1"]
            * (2 / 57.07)
        ) / 60

    if f"PRODUTI MDOT PRENSA - US{plant_number}" in tags_needed:
        p = df_sql["Vel Periferica R Fixo [m/s]"] > 0
        df_sql[f"PRODUTI MDOT PRENSA - US{plant_number}"] = df_sql[
            f"PESO1_I@{plant_number_long}PR-BW-822I-01M1"
        ][p] / (2 * 1.5 * df_sql["Vel Periferica R Fixo [m/s]"][p])

    if "DESV MEDIO ALT CAMADA" in tags_needed:
        df_sql["DESV MEDIO ALT CAMADA"] = df_sql[
            [
                f"NIVE1_C@{plant_number_long}QU-FR-851I-01M1",
                f"NIVE2_I@{plant_number_long}QU-FR-851I-01M1",
                f"NIVE3_I@{plant_number_long}QU-FR-851I-01M1",
                f"NIVE4_I@{plant_number_long}QU-FR-851I-01M1",
                f"NIVE5_I@{plant_number_long}QU-FR-851I-01M1",
                f"NIVE6_I@{plant_number_long}QU-FR-851I-01M1",
            ]
        ].std(axis=1)

    if f"PERMEABILIDADE CV18 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV18 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-18"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"PV TEMP GQ3-16-MED - US{plant_number}" in tags_needed:
        df_sql[f"PV TEMP GQ3-16-MED - US{plant_number}"] = np.mean(
            [
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ03"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ04"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ05"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ06"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ06"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ07"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ08"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ09"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ10"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ11"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ12"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ13"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ14"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ15"],
                df_sql[f"TEMP1_I@{plant_number_long}QU-QU-855I-GQ16"],
            ],
            axis=0,
        )

    if f"PROD FILTR - US{plant_number}" in tags_needed:
        df_sql[f"PROD FILTR - US{plant_number}"] = (
            df_sql[f"PESO1_I@{plant_number_long}FI-TR-827I-01M1"]
            + df_sql[f"PESO1_I@{plant_number_long}FI-TR-827I-02M1"]
        )

    if f"PERMEABILIDADE CV19 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV19 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-19"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"Nº FILTR FUNC - US{plant_number}" in tags_needed:
        df_sql[f"Nº FILTR FUNC - US{plant_number}"] = (
            df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-01"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-02"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-03"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-04"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-05R"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-06"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-07"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-08"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-09"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-10R"]
        )

    if f"Nº BV FUNC - US{plant_number}" in tags_needed:
        df_sql[f"Nº BV FUNC - US{plant_number}"] = (
            df_sql[f"FUNC1_D@{plant_number_long}FI-BV-827I-01M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-BV-827I-02M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-BV-827I-03M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-BV-827I-04M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-BV-827I-05RM1"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-BV-827I-06M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-BV-827I-07M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-BV-827I-08M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-BV-827I-09M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-BV-827I-10RM1"]
        )

    if f"PERMEABILIDADE CV3A - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV3A - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-03A"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if "Vel Periferica R Móvel [m/s]" in tags_needed:
        df_sql["Vel Periferica R Móvel [m/s]"] = (
            math.pi
            * df_sql[f"ROTA1_I@{plant_number_long}PR-RP-822I-01M2"]
            * (2 / 57.07)
        ) / 60

    if "Paralelismo" in tags_needed:
        df_sql["Paralelismo"] = np.abs(
            df_sql[f"POSI1_I@{plant_number_long}PR-RP-822I-01"]
            - df_sql[f"POSI2_I@{plant_number_long}PR-RP-822I-01"]
        )

    if f"PERMEABILIDADE CV15 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV15 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-15"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"PERMEABILIDADE CV3B - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV3B - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-03B"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"PERMEABILIDADE CV2 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV2 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-02"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"PERMEABILIDADE CV12 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV12 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-12"]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if "MAIOR - MENOR ALT CAMADA" in tags_needed:
        df_sql["MAIOR - MENOR ALT CAMADA"] = np.max(
            [
                df_sql[f"NIVE1_C@{plant_number_long}QU-FR-851I-01M1"],
                df_sql[f"NIVE2_I@{plant_number_long}QU-FR-851I-01M1"],
                df_sql[f"NIVE3_I@{plant_number_long}QU-FR-851I-01M1"],
                df_sql[f"NIVE4_I@{plant_number_long}QU-FR-851I-01M1"],
                df_sql[f"NIVE5_I@{plant_number_long}QU-FR-851I-01M1"],
                df_sql[f"NIVE6_I@{plant_number_long}QU-FR-851I-01M1"],
            ],
            axis=0,
        ) - np.min(
            [
                df_sql[f"NIVE1_C@{plant_number_long}QU-FR-851I-01M1"],
                df_sql[f"NIVE2_I@{plant_number_long}QU-FR-851I-01M1"],
                df_sql[f"NIVE3_I@{plant_number_long}QU-FR-851I-01M1"],
                df_sql[f"NIVE4_I@{plant_number_long}QU-FR-851I-01M1"],
                df_sql[f"NIVE5_I@{plant_number_long}QU-FR-851I-01M1"],
                df_sql[f"NIVE6_I@{plant_number_long}QU-FR-851I-01M1"],
            ],
            axis=0,
        )

    if f"PERMEABILIDADE CV5 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV5 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-05"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"PERMEABILIDADE CV9 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV9 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-09"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"PERMEABILIDADE CV7 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV7 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-07"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"GANHO PRENSA - US{plant_number}" in tags_needed:
        df_sql[f"GANHO PRENSA - US{plant_number}"] = (
            df_sql[f"SUP_SE_PP_L@{plant_number_long}PR"]
            - df_sql[f"SUP_SE_PR_L@{plant_number_long}FI"]
        )

    if f"PERMEABILIDADE CV31 - CALC - US{plant_number}" in tags_needed:
        df_sql[f"PERMEABILIDADE CV31 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-31"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if "Escorregamento" in tags_needed:
        p1 = df_sql[f"PESO1_I@{plant_number_long}PR-BW-822I-01M1"] > 0
        p2 = df_sql[f"PRES3_I@{plant_number_long}PR-RP-822I-01"] > 0
        df_sql["Escorregamento"] = (
            1000
            * df_sql[f"ROTA1_I@{plant_number_long}PR-RP-822I-01M1"][p1 & p2]
            / df_sql[f"PESO1_I@{plant_number_long}PR-BW-822I-01M1"][p1 & p2]
            / df_sql[f"PRES3_I@{plant_number_long}PR-RP-822I-01"][p1 & p2]
        )

    if f"PERMEABILIDADE CV8 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV8 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-08"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"GAP CALC PRENSA - US{plant_number}" in tags_needed:
        p = df_sql["Vel Periferica R Fixo [m/s]"] > 0
        df_sql[f"GAP CALC PRENSA - US{plant_number}"] = (
            df_sql[f"PESO1_I@" f"{plant_number_long}PR-BW-822I-01M1"][p]
            / (1.5 * 3.5 * df_sql["Vel " "Periferica R Fixo [m/s]"][p])
        ) / 3.6

    if f"CONS EE PRENSA - US{plant_number}" in tags_needed:
        df_sql[f"CONS EE PRENSA - US{plant_number}"] = df_sql[
            f"CONS1_Y@{plant_number_long}PR-RP-822I-01"
        ]

    if f"PERMEABILIDADE CV16 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV16 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-16"]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"POT TOTAL VENT - US{plant_number}" in tags_needed:
        df_sql[f"POT TOTAL VENT - US{plant_number}"] = (
            df_sql[f"POTE1_I@{plant_number_long}QU-PF-852I-01M1"]
            + df_sql[f"POTE1_I@{plant_number_long}QU-PF-852I-02M1"]
            + df_sql[f"POTE1_I@{plant_number_long}QU-PF-852I-03M1"]
            + df_sql[f"POTE1_I@{plant_number_long}QU-PF-852I-04M1"]
            + df_sql[f"POTE1_I@{plant_number_long}QU-PF-852I-05M1"]
            + df_sql[f"POTE1_I@{plant_number_long}QU-PF-852I-06M1"]
        )

    if f"SOMA POTENCIA BV - US{plant_number}" in tags_needed:
        df_sql[f"SOMA POTENCIA BV - US{plant_number}"] = (
            df_sql[f"POTE1_I@{plant_number_long}FI-BV-827I-01M1"]
            + df_sql[f"POTE1_I@{plant_number_long}FI-BV-827I-02M1"]
            + df_sql[f"POTE1_I@{plant_number_long}FI-BV-827I-03M1"]
            + df_sql[f"POTE1_I@{plant_number_long}FI-BV-827I-04M1"]
            + df_sql[f"POTE1_I@{plant_number_long}FI-BV-827I-05RM1"]
            + df_sql[f"POTE1_I@{plant_number_long}FI-BV-827I-06M1"]
            + df_sql[f"POTE1_I@{plant_number_long}FI-BV-827I-07M1"]
            + df_sql[f"POTE1_I@{plant_number_long}FI-BV-827I-08M1"]
            + df_sql[f"POTE1_I@{plant_number_long}FI-BV-827I-09M1"]
            + df_sql[f"POTE1_I@{plant_number_long}FI-BV-827I-10RM1"]
        )

    if f"Calculo da Energia da Filtragem" in tags_needed:
        p = df_sql[f"PROD FILTR - US{plant_number}"] > 0
        df_sql[f"Calculo da Energia da Filtragem"] = df_sql[
            f"CONS1_Y@{plant_number_long}FI-BV-827I"
        ]

    if f"PERMEABILIDADE CV4 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV4 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-04"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if "Consumo de Energia (base minério úmido) kWh/ton" in tags_needed:
        df_sql["Consumo de Energia (base minério úmido) kWh/ton"] = df_sql[
            f"CONS1_Y@{plant_number_long}MO-MOAG"
        ]

    if f"RPM MED FILTROS - US{plant_number}" in tags_needed:
        df_sql[f"RPM MED FILTROS - US{plant_number}"] = np.mean(
            [
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-01M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-02M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-03M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-04M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-05RM1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-06M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-07M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-08M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-09M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-10RM1"],
            ],
            axis=0,
        )

    if f"PERMEABILIDADE CV6 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV6 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-06"][p]
            / df_sql["PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if "SOMA FUNC FILTROS" in tags_needed:
        df_sql["SOMA FUNC FILTROS"] = (
            df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-01"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-02"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-03"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-04"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-05R"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-06"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-07"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-08"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-09"]
            + df_sql[f"FUNC1_D@{plant_number_long}FI-FL-827I-10R"]
        )

    if "Produtividade Pelotamento Virtual" in tags_needed:
        p = df_sql["SOMA FUNC FILTROS"] > 0
        df_sql["Produtividade Pelotamento Virtual"] = (
            df_sql[f"PROD_PC_I@" f"{plant_number_long}US"][p]
            - df_sql[f"PESO1_I@{plant_number_long}PE-TR-851I-03M1"][p]
        ) / ((7.5 / 2) * math.pi * df_sql["SOMA FUNC " "FILTROS"][p])

    if f"Produtividade Pel Efetiva" in tags_needed:
        p = df_sql["SOMA FUNC FILTROS"] > 0
        df_sql[f"Produtividade Pel Efetiva"] = (
            df_sql[f"PROD_PQ_Y@" f"{plant_number_long}US"][p]
        ) / (((7.5 / 2) * math.pi) * df_sql["SOMA FUNC FILTROS"][p])

    if f"10 - DIF PRODUTIVI EFETIVA - VIRTUAL - CALC - US{plant_number}" in tags_needed:
        df_sql[
            f"10 - DIF PRODUTIVI EFETIVA - VIRTUAL - CALC - US{plant_number}"
        ] = (
            df_sql["Produtividade Pel Efetiva"]
            - df_sql["Produtividade Pelotamento Virtual"]
        )

    if f"PERMEABILIDADE CV10 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV10 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-10"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"PERMEABILIDADE CV20 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV20 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-20"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"PERMEABILIDADE CV14 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV14 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-14"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"PERMEABILIDADE CV21 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV21 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-21"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"PERMEABILIDADE CV17 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV17 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-17"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"PERMEABILIDADE CV11 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV11 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-11"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"CONS ESPEC EE VENT - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"CONS ESPEC EE VENT - US{plant_number}"] = (
            df_sql[f"POT TOTAL VENT - US{plant_number}"][p]
            / df_sql[f"PfROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"PERMEABILIDADE CV1 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV1 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-01"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if f"MAIOR TEMP PQ - US{plant_number}" in tags_needed:
        df_sql[f"MAIOR TEMP PQ - US{plant_number}"] = np.max(
            [
                df_sql[f"TEMP1_I@{plant_number_long}PN-TR-860I-01"],
                df_sql[f"TEMP1_I@{plant_number_long}PN-TR-860I-02"],
            ],
            axis=0,
        )

    if f"PERMEABILIDADE CV13 - CALC - US{plant_number}" in tags_needed:
        p = df_sql[f"PROD_PQ_Y@{plant_number_long}US"] > 0
        df_sql[f"PERMEABILIDADE CV13 - CALC - US{plant_number}"] = (
            df_sql[f"PRES1_I@{plant_number_long}QU-WB-851I-13"][p]
            / df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
        )

    if "vazao_antracito" in tags_needed:
        df_sql["vazao_antracito"] = (
            df_sql[f"PESO1_I@{plant_number_long}MO-BW-813I-03M1"]
            + df_sql[f"PESO1_I@{plant_number_long}MO-BW-813I-04M1"]
        )

    if "%CF. ANT" in tags_needed:
        df_sql["%CF. ANT"] = (
            df_sql[f"QUIM_CFIX_PP_L@{plant_number_long}PR"]
            / df_sql[f"QUIM_CARVAO_PP_L@{plant_number_long}PR"]
        )

    if f"=192/VELO" in tags_needed:
        p = df_sql[f"VELO1_C@{plant_number_long}QU-FR-851I-01M1"] > 0
        df_sql["=192/VELO"] = (
            192 / df_sql[f"VELO1_C@{plant_number_long}QU-FR-851I-01M1"][p]
        )

    if "=PQ*24/768/FUNC" in tags_needed:
        p = df_sql[f"FUNC1_D@{plant_number_long}QU-FR-851I-01M1"] > 0
        df_sql["=PQ*24/768/FUNC"] = (
            df_sql[f"PROD_PQ_Y@{plant_number_long}US"][p]
            * 24
            / 768
            / df_sql[f"FUNC1_D@{plant_number_long}QU-FR-851I-01M1"][p]
        )

    if "media vel de disco de pel" in tags_needed:
        df_sql["media vel de disco de pel"] = (
            (
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-01M1"]
                * df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-01M1"]
            )
            + (
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-02M1"]
                * df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-02M1"]
            )
            + (
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-03M1"]
                * df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-03M1"]
            )
            + (
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-04M1"]
                * df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-04M1"]
            )
            + (
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-05M1"]
                * df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-05M1"]
            )
            + (
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-06M1"]
                * df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-06M1"]
            )
            + (
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-07M1"]
                * df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-07M1"]
            )
            + (
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-08M1"]
                * df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-08M1"]
            )
            + (
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-09M1"]
                * df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-09M1"]
            )
            + (
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-10M1"]
                * df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-10M1"]
            )
            + (
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-11M1"]
                * df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-11M1"]
            )
            + (
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-12M1"]
                * df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-12M1"]
            )
        ) / (
            df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-01M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-02M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-03M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-04M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-05M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-06M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-07M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-08M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-09M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-10M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-11M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}PE-BD-840I-12M1"]
        )

    if "media gap prensa" in tags_needed:
        df_sql["media gap prensa"] = np.mean(
            [
                df_sql[f"POSI1_I@{plant_number_long}PR-RP-822I-01"],
                df_sql[f"POSI2_I@{plant_number_long}PR-RP-822I-01"],
            ],
            axis=0,
        )

    if "media rolo fixo prensa" in tags_needed:
        df_sql["media rolo fixo prensa"] = np.mean(
            [
                df_sql[f"POTE1_I@{plant_number_long}PR-RP-822I-01M1"],
                df_sql[f"POTE1_I@{plant_number_long}PR-RP-822I-01M2"],
            ],
            axis=0,
        )

    if "media oleo prensa" in tags_needed:
        df_sql["media oleo prensa"] = np.mean(
            [
                df_sql[f"PRES2_I@{plant_number_long}PR-RP-822I-01"],
                df_sql[f"PRES3_I@{plant_number_long}PR-RP-822I-01"],
            ],
            axis=0,
        )
    if "media balanca moinho" in tags_needed:
        df_sql["media balanca moinho"] = np.mean(
            [
                df_sql[f"PESO1_I@{plant_number_long}MO-BW-821I-01M1"],
                df_sql[f"PESO1_I@{plant_number_long}MO-BW-821I-02M1"],
                df_sql[f"PESO1_I@{plant_number_long}MO-BW-821I-03M1"],
            ],
            axis=0,
        )

    if "media_vazao_antracito" in tags_needed:
        df_sql["media_vazao_antracito"] = np.mean(
            [
                df_sql[f"PESO1_I@{plant_number_long}MO-BW-813I-03M1"],
                df_sql[f"PESO1_I@{plant_number_long}MO-BW-813I-04M1"],
            ],
            axis=0,
        )

    if "soma balanca minerio misturador" in tags_needed:
        df_sql["soma balanca minerio misturador"] = (
            df_sql[f"PESO1_I@{plant_number_long}MI-BW-832I-01M1"]
            + df_sql[f"PESO1_I@{plant_number_long}MI-BW-832I-02M1"]
        )

    if "soma balanca bentonita misturador" in tags_needed:
        df_sql["soma balanca bentonita misturador"] = (
            df_sql[f"PESO1_I@{plant_number_long}MI-LW-832I-01M1"]
            + df_sql[f"PESO1_I@{plant_number_long}MI-LW-832I-02M1"]
        )

    if "soma balanca retorno correia" in tags_needed:
        df_sql["soma balanca retorno correia"] = (
            df_sql[f"PESO1_I@{plant_number_long}PE-TR-840I-28M1"]
            + df_sql[f"PESO1_I@{plant_number_long}PE-TR-840I-29M1"]
        )

    if "media temp pelotas" in tags_needed:
        df_sql["media temp pelotas"] = np.mean(
            [
                df_sql[f"TEMP1_I@{plant_number_long}PN-TR-860I-01"],
                df_sql[f"TEMP1_I@{plant_number_long}PN-TR-860I-02"],
            ],
            axis=0,
        )

    if "media disco de pelotamento" in tags_needed:
        df_sql["media disco de pelotamento"] = np.mean(
            [
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-01M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-02M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-03M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-04M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-05M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-06M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-07M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-08M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-09M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-10M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-11M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-12M1"],
            ],
            axis=0,
        )

    if "calc_distribuicao_gran" in tags_needed:
        df_sql["calc_distribuicao_gran"] = (
            df_sql[f"GRAN_12,5_PQ_L@{plant_number_long}QU"]
            + df_sql[f"GRAN_16_PQ_L@{plant_number_long}QU"]
            + df_sql[f"GRAN_10_PQ_L@{plant_number_long}QU"]
            + df_sql[f"GRAN_8_PQ_L@{plant_number_long}QU"]
        )

    if "bomba de retorno tanque" in tags_needed:
        df_sql["bomba de retorno tanque"] = df_sql[
            f"VAZA1_I@{plant_number_long}MO-BP-821I-01"
        ] / (
            df_sql[f"VAZA1_I@{plant_number_long}MO-BP-821I-01"]
            + df_sql[f"VAZA1_I@{plant_number_long}MO-BP-821I-01M1"]
        )

    if "media de densidade" in tags_needed:
        df_sql["media de densidade"] = (
            (
                df_sql[f"DENS1_C@{plant_number_long}HO-BP-826I-05"]
                * df_sql[f"FUNC1_D@{plant_number_long}HO-BP-826I-05M1"]
            )
            + (
                df_sql[f"DENS1_C@{plant_number_long}HO-BP-826I-06R"]
                * df_sql[f"FUNC1_D@{plant_number_long}HO-BP-826I-06RM1"]
            )
            + (
                df_sql[f"DENS1_C@{plant_number_long}HO-BP-826I-07"]
                * df_sql[f"FUNC1_D@{plant_number_long}HO-BP-826I-07M1"]
            )
            + (
                df_sql[f"DENS1_C@{plant_number_long}HO-BP-826I-08R"]
                * df_sql[f"FUNC1_D@{plant_number_long}HO-BP-826I-08RM1"]
            )
        ) / (
            df_sql[f"FUNC1_D@{plant_number_long}HO-BP-826I-05M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}HO-BP-826I-06RM1"]
            + df_sql[f"FUNC1_D@{plant_number_long}HO-BP-826I-07M1"]
            + df_sql[f"FUNC1_D@{plant_number_long}HO-BP-826I-08RM1"]
        )

    if "mediana de rotacao" in tags_needed:
        df_sql["mediana de rotacao"] = np.median(
            [
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-01M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-02M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-03M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-04M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-05RM1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-06M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-07M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-08M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-09M1"],
                df_sql[f"ROTA1_I@{plant_number_long}FI-FL-827I-10RM1"],
            ],
            axis=0,
        )
    if "media disco de pelotamento" in tags_needed:
        df_sql["media disco de pelotamento"] = np.mean(
            [
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-01M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-02M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-03M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-04M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-05M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-06M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-07M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-08M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-09M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-10M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-11M1"],
                df_sql[f"ROTA1_I@{plant_number_long}PE-BD-840I-12M1"],
            ],
            axis=0,
        )
    if "media alimentacao do disco" in tags_needed:
        df_sql["media alimentacao do disco"] = np.mean(
            [
                df_sql[f"PESO1_I@{plant_number_long}PE-BW-840I-01M1"],
                df_sql[f"PESO1_I@{plant_number_long}PE-BW-840I-02M1"],
                df_sql[f"PESO1_I@{plant_number_long}PE-BW-840I-03M1"],
                df_sql[f"PESO1_I@{plant_number_long}PE-BW-840I-04M1"],
                df_sql[f"PESO1_I@{plant_number_long}PE-BW-840I-05M1"],
                df_sql[f"PESO1_I@{plant_number_long}PE-BW-840I-06M1"],
                df_sql[f"PESO1_I@{plant_number_long}PE-BW-840I-07M1"],
                df_sql[f"PESO1_I@{plant_number_long}PE-BW-840I-08M1"],
                df_sql[f"PESO1_I@{plant_number_long}PE-BW-840I-09M1"],
                df_sql[f"PESO1_I@{plant_number_long}PE-BW-840I-10M1"],
                df_sql[f"PESO1_I@{plant_number_long}PE-BW-840I-11M1"],
                df_sql[f"PESO1_I@{plant_number_long}PE-BW-840I-12M1"],
            ],
            axis=0,
        )

    if "media tm" in tags_needed:
        df_sql["media tm"] = np.mean(
            [
                df_sql[f"GRAN_OCS_TM@{plant_number_long}PE-BD-840I-01"],
                df_sql[f"GRAN_OCS_TM@{plant_number_long}PE-BD-840I-02"],
                df_sql[f"GRAN_OCS_TM@{plant_number_long}PE-BD-840I-03"],
                df_sql[f"GRAN_OCS_TM@{plant_number_long}PE-BD-840I-04"],
                df_sql[f"GRAN_OCS_TM@{plant_number_long}PE-BD-840I-05"],
                df_sql[f"GRAN_OCS_TM@{plant_number_long}PE-BD-840I-06"],
                df_sql[f"GRAN_OCS_TM@{plant_number_long}PE-BD-840I-07"],
                df_sql[f"GRAN_OCS_TM@{plant_number_long}PE-BD-840I-08"],
                df_sql[f"GRAN_OCS_TM@{plant_number_long}PE-BD-840I-09"],
                df_sql[f"GRAN_OCS_TM@{plant_number_long}PE-BD-840I-10"],
                df_sql[f"GRAN_OCS_TM@{plant_number_long}PE-BD-840I-11"],
                df_sql[f"GRAN_OCS_TM@{plant_number_long}PE-BD-840I-12"],
            ],
            axis=0,
        )

    if "media_potencia_moinho" in tags_needed:
        df_sql["media_potencia_moinho"] = np.mean(
            [
                df_sql[f"POTE1_I@{plant_number_long}MO-MO-821I-01M1"],
                df_sql[f"POTE1_I@{plant_number_long}MO-MO-821I-02M1"],
                df_sql[f"POTE1_I@{plant_number_long}MO-MO-821I-03M1"],
            ],
            axis=0,
        )

    if f"NIVE1_I@{plant_number_long}HO-TQ-826I-03" in tags_needed:
        p = df_sql[f"FUNC1_D@{plant_number_long}HO-AG-826I-01M1"] > 0
        df_sql[f"NIVE1_I@{plant_number_long}HO-TQ-826I-03"] = df_sql[
            f"NIVE1_I@{plant_number_long}HO-TQ-826I-03"
        ][p]

    if f"NIVE1_I@{plant_number_long}HO-TQ-826I-04" in tags_needed:
        p = df_sql[f"FUNC1_D@{plant_number_long}HO-AG-826I-02M1"] > 0
        df_sql[f"NIVE1_I@{plant_number_long}HO-TQ-826I-04"] = df_sql[
            f"NIVE1_I@{plant_number_long}HO-TQ-826I-04"
        ][p]

    if f"NIVE1_I@{plant_number_long}HO-TQ-826I-05" in tags_needed:
        p = df_sql[f"FUNC1_D@{plant_number_long}HO-AG-826I-03M1"] > 0
        df_sql[f"NIVE1_I@{plant_number_long}HO-TQ-826I-05"] = df_sql[
            f"NIVE1_I@{plant_number_long}HO-TQ-826I-05"
        ][p]

    if "corpo_moedor_especifico" in tags_needed:
        df_sql["corpo_moedor_especifico"] = (
            (
                df_sql[f"PESO1_Q@{plant_number_long}MO-TR-821I-02M1-MO01"]
                + df_sql[f"PESO1_Q@{plant_number_long}MO-TR-821I-02M1-MO02"]
                + df_sql[f"PESO1_Q@{plant_number_long}MO-TR-821I-02M1-MO03"]
            )
            * df_sql[f"FUNC1_D@{plant_number_long}QU-FR-851I-01M1"]
            * 1000
            / df_sql[f"PROD_PQ_Y@{plant_number_long}QU-ACUM"]
        )

    if "ProducaoPQ_Moagem" in tags_needed:
        df_sql["taxa_alimentacao_moinhos"] = (
            df_sql[f"PESO1_I@{plant_number_long}MO-BW-821I-01M1"]
            + df_sql[f"PESO1_I@{plant_number_long}MO-BW-821I-02M1"]
            + df_sql[f"PESO1_I@{plant_number_long}MO-BW-821I-03M1"]
        )
        df_sql["ProducaoPQ_Moagem"] = 0.8762 * df_sql["taxa_alimentacao_moinhos"]

    if "antracito" in tags_needed:
        df_sql["antracito"] = df_sql[f"QUIM_CARVAO_PP_L@{plant_number_long}PR"] * 10

    if "calcario" in tags_needed:
        df_sql["denominator"] = 0.8762 * (
            df_sql[f"PESO1_I@{plant_number_long}MO-BW-821I-01M1"]
            + df_sql[f"PESO1_I@{plant_number_long}MO-BW-821I-02M1"]
            + df_sql[f"PESO1_I@{plant_number_long}MO-BW-821I-03M1"]
        )
        df_sql["calcario"] = df_sql[f"PESO1_I@{plant_number_long}MO-BW-813I-01M1"] / df_sql["denominator"]

    return df_sql
