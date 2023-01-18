"""
Define variables needed to run the application.

This module also defines the ``limits`` variable, that uses the ``limits_file``
to define the optimization problem boundaries.

"""
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd


limits = {
    "GRAN PR": None,
    "SE PP": None,
    "SE PR": None,
    "SUP_SE_PP": None,
    "abrasao": ["LT", 5.5],
    "basicidade": ["GT", 0.95],
    "cfix": ["LTE", 1.26],
    "compressao": ["GT", 290],
    "custo_GRAN PR": None,
    "custo_SE PP": None,
    "custo_SE PR": None,
    "dens_moinho_1": None,
    "dens_moinho_2": None,
    "dens_moinho_3": None,
    "distribuicao gran": ["GTE", 95],
    "energia_forno": None,
    "energia_moinho": None,
    "energia_prensa": ["LT", 35],
    "finos": ["LT", 1],
    "gas": None,
    "particulados1": ["LT", 45],
    "particulados2": ["LT", 45],
    "particulados3": ["LT", 45],
    "produtividade filtragem": None,
    "relacao gran": None,
    "taxarp": ["LT", 35],
    "temp_forno": ('LTE', 180),
    "temp_precipitador_1": None,
    "temp_precipitador_2": None,
    "temp_precipitador_3": None,
    "temp_precipitador_4": None,
    "temp_recirc": None,
    "torque": None,
    "umidade": None,
    #     "energia_filtro": None,
}


class constants:
    """ Defining optimizer constants"""

    @staticmethod
    def _read_limits(limits):
        return {
            key: tuple(value) if value else value for key, value in
            limits.items()
        }

    @staticmethod
    def _scaling_limits(limits, tags):
        """Normalize tags values using ``np.log``.

        Parameters
        ----------
        limits : dict
            Dictionary with the optimization problem boundaries.
        tags : List[str]
            List of tags to normalize.
        """
        for tag in tags:
            tag_limits = limits[tag]
            if (
                isinstance(tag_limits, Iterable)
                and not isinstance(tag_limits, str)
            ):
                limits[tag] = (tag_limits[0], np.log(tag_limits[1]))
        return limits

    @staticmethod
    def define_targets(datasets: Dict[str, pd.DataFrame]) -> List[str]:
        """
        Return list of values to consider as the models targets.

        Method considers the last column of each dataframe
        from :param:`datasets` as the target.

        Parameters
        ----------
        datasets : Dict[str, pd.DataFrame]
            Dictionary with the datasets to retrieve the targets from.

        Returns
        -------
        list
            List of targets.
        """
        return [df.columns[-1] for df in datasets.values()]

    @staticmethod
    def _additional_features(limits):
        features = {"custo_abrasao": "abrasao",
                    "custo_compressao": "compressao",
                    "custo_distribuicao gran": "distribuicao gran"}

        for feature, value in features.items():
            limits[feature] = limits[value]

        return limits

    production_range = [(prod_range, prod_range + 50)
                        for prod_range in range(700, 1000, 50)]

    tags_to_scale = ['compressao', 'particulados1', 'particulados2',
                     'particulados3', 'custo_compressao']
    LIMITS = _read_limits.__func__(limits)
    LIMITS = _additional_features.__func__(LIMITS)
    LIMITS = _scaling_limits.__func__(LIMITS, tags_to_scale)

    # tags that are target in one model and descriptive feature in others
    # old tag_2_var
    _POWER_TOKEN = 'Consumo de Energia (base minério úmido) kWh/ton '

    TARGETS_IN_MODEL = {
        'GANHO PRENSA - US8': 'SE PP',
        'SUP_SE_PP_L@08PR': 'SUP_SE_PP',
        'GRAN_-0,045_PR_L@08FI': 'GRAN PR',
        'SUP_SE_PR_L@08FI': 'SE PR',
        'VAZA3_I@08QU-ST-855I-01': 'gas',
        'Consumo de Energia (base minério úmido) kWh/ton': 'energia_moinho',
        'CONS EE PRENSA - US8': 'energia_prensa',
        'Calculo da Energia da Filtragem': 'energia_filtro',
        'CONS ESP VENT TOTAL - US8': 'energia_forno',
        'TORQ1': 'torque',
        'TEMP1_I@08QU-HO-851I-01': 'temp_recirc',
        'QUIM_CFIX_PP_L@08PR': 'cfix',
        'UMID_H2O_PR_L@08FI': 'umidade',
        'COMP_MCOMP_PQ_L@08QU': 'compressao',
        'ABRA_-0,5_PQ_L@08QU': 'abrasao',
        'PARTIC_I@08QU-CH-854I-01': 'particulados1',
        'PARTIC_I@08QU-CH-854I-02': 'particulados2',
        'PARTIC_I@08QU-CH-854I-03': 'particulados3',
        'RETO1_Y@08PE': 'taxarp',
        'CALC1_Y@08FI-FD00': 'produtividade filtragem',
        'TEMP1_I@08QU-PP-871I-01': 'temp_precipitador_1',
        'TEMP1_I@08QU-PP-871I-02': 'temp_precipitador_2',
        'TEMP1_I@08QU-PP-871I-03': 'temp_precipitador_3',
        'TEMP1_I@08PP-DU-872I-01-93': 'temp_precipitador_4',
        'rel_gran': 'relacao gran',
        **{f'ROTA1_I@08PE-BD-840I-{i:02}M1': f'rota_disco_{i}'
           for i in range(1, 13)},
        **{f'DENS1_C@08MO-MO-821I-{i:02}': f'dens_moinho_{i}'
           for i in range(1, 4)}
        #                         'QUIM_BAS2_PQ_L@08QU': 'basicidade',
        #                         'GRAN_-5_PQ_L@08QU': 'finos',
        #                         _POWER_TOKEN + '1': 'energia_moinho1',
        #                         _POWER_TOKEN + '2': 'energia_moinho2',
        #                         _POWER_TOKEN + '3': 'energia_moinho3',
    }

    OBJ_FUNC_COEF = [
        'Calculo da Energia da Filtragem',
        'antracito',
        'bentonita',
        'calcario',
        'corpo_moedor_especifico',
        'energia_forno',
        'energia_moinho',
        'energia_prensa',
        'gas',
        #                       'POTE1_I@08FI-BV-827I-01M1',
        #                       'POTE1_I@08FI-BV-827I-02M1',
        #                       'POTE1_I@08FI-BV-827I-03M1',
        #                       'POTE1_I@08FI-BV-827I-04M1',
        #                       'POTE1_I@08FI-BV-827I-05RM1',
        #                       'POTE1_I@08FI-BV-827I-06M1',
        #                       'POTE1_I@08FI-BV-827I-08M1',
        #                       'POTE1_I@08FI-BV-827I-09M1',
        #                       'POTE1_I@08FI-BV-827I-10RM1',
        #                       'corpo_moedor_especifico_1',
        #                       'corpo_moedor_especifico_2',
        #                       'corpo_moedor_especifico_3',
        #                       'energia_moinho1',
        #                       'energia_moinho2',
        #                       'energia_moinho3',
    ]
