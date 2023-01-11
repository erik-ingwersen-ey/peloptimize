range_constraints = {
    "media_nivel": [{"feature": "NIVE{}_I@08QU-FR-851I-01M1", "start": 2, "end": 6, "coef": -0.20},
                    {"feature": "NIVE1_C@08QU-FR-851I-01M1", "coef": 1},
                    {"operator": "E", "coef": 0}],
  
    "qtde_discos": [{"feature": "qtde_discos", "coef": 1},
                    {"feature": "FUNC1_D@08PE-BD-840I-{:02}M1", "coef": -1, "start": 1, "end": 12},
                    {"operator": "E", "coef": 0}
                   ],
  
    "qtde_moinhos": [{"feature": "qtde_moinhos", "coef": 1},
                  {"feature": "FUNC1_D@08MO-MO-821I-{:02}M1", "coef": -1, "start": 1, "end": 3},
                  {"operator": "E", "coef": 0}
                 ],
  
#     "qtde_filtros": [{"feature": "qtde_filtros", "coef": 1},
#                   {"feature": "FUNC1_D@08FI-FL-827I-{:02}", "coef": -1, "start": 1, "end": 4},
#                   {"feature": "FUNC1_D@08FI-FL-827I-{:02}", "coef": -1, "start": 6, "end": 9},
#                   {"feature": "FUNC1_D@08FI-FL-827I-05R", "coef": -1},
#                   {"feature": "FUNC1_D@08FI-FL-827I-10R", "coef": -1},
#                   {"operator": "E", "coef": 0}
#                  ],
#     "med_rot_disc_funcionais": [{"feature": "rota_disco_{:01}", "start": 1, "end": 12, "coef": -1},
#                                 {"feature": "Media_ROTA1_I_PE-BD-840I-01_12", "coef": 12},
#                                 {"operator": "E", "coef": 0}],
#     "agitador_homog": [{"feature": "Soma_Func_Agitador_Tanque_Homog", "coef": 3},
#                        {"feature": "FUNC1_D@08HO-AG-826I-{:02}M1", "start": 1, "end": 3, "coef": -1},
#                        {"operator": "E", "coef": 0}],
#     "alim_moinho_avg": [{"feature": "PESO1_I@08MO-BW-821I-{:02}M1", "start": 1, "end": 3, "coef": -1},
#                         {"feature": "alimentacao_moinho_media", "coef": 3},
#                         {"operator": "E", "coef": 0}],
#     "tanques_rota_media": [{"feature": "ROTA1_I@08HO-AG-826I-{:02}M1", "start": 1, "end": 3, "coef": -1},
#                            {"feature": "tanques_rota_avg", "coef": 3},
#                            {"operator": "E", "coef": 0}],
#     "tanques_nivel_media": [{"feature": "NIVE1_I@08HO-TQ-826I-{:02}", "start": 3, "end": 5, "coef": -1},
#                             {"feature": "Media_NIVE1_I_HO-TQ-826I-03_05", "coef": 3},
#                             {"operator": "E", "coef": 0}],
#     "perm_3b_8_media": [{"feature": "perm_3b_8", "coef": 6},
#                         {"feature": "PRES1_I@08QU-WB-851I-03B", "coef": -1},
#                         {"feature": "PRES1_I@08QU-WB-851I-{:02}", "start": 4, "end": 8, "coef": -1},
#                         {"operator": "E", "coef": 0}],
#     "perm_9_13_media": [{"feature": "perm_9_13", "coef": 5},
#                         {"feature": "PRES1_I@08QU-WB-851I-{:02}", "start": 9, "end": 13, "coef": -1},
#                         {"operator": "E", "coef": 0}],
#     "gran_pe_bd_media": [{"feature": "GRAN_OCS_TM@08PE-BD-840I-{:02}", "start": 1, "end": 12, "coef": -1},
#                          {"feature": "granularidadeAvg", "coef": 12},
#                          {"operator": "E", "coef": 0}],
#     "agitador_homog": [{"feature": "Soma_Func_Agitador_Tanque_Homog", "coef": 3},
#                        {"feature": "FUNC1_D@08HO-AG-826I-{:02}M1", "start": 1, "end": 3, "coef": -1},
#                        {"operator": "E", "coef": 0}]
}


