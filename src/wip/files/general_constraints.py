general_constraints = {
#     "pressoes_iguais": [["PRES3_I@08PR-RP-822I-01", 1], ["PRES2_I@08PR-RP-822I-01", -1], ["E", 0]],
#     "ganho_equality": [["SUP_SE_PP_L@08PR", -1], ["SUP_SE_PR_L@08FI", -1], ["SE PP", 1], ["E", 0]],
#     "rotacao_motores_iguais": [["ROTA1_I@08PR-RP-822I-01M1", 1], ["ROTA1_I@08PR-RP-822I-01M2", -1], ["E", 0]],
#     "func_rot_8": [["FUNC1_D@08FI-FL-827I-08", -1], ["FUNC1_D@08FI-BV-827I-08M1", 1], ["E", 0]],
#     "taxas": [["RETO1_Y@08PE", 1], ["taxarp", -1], ["E", 0]],
#     "pote_prensa_iguais": [["POTE1_I@08PR-RP-822I-01M1", 1], ["POTE1_I@08PR-RP-822I-01M2", -1], ["E", 0]],    
#     "med_vazao": [["VAZA1_I@08PE-40F32", -1], ["vazaoAVG", 12], ["E", 0]],
#     "britadores": [["Soma_Func_Britador", 2], ["FUNC1_D@08AD-BR-813I-01M1", -1], ["FUNC1_D@08AD-BR-813I-02M1", -1], ["E", 0]],
#     "MO_813I": [["Soma_Func_MO_813I", 2], ["FUNC1_D@08MO-BW-813I-04M1", -1], ["FUNC1_D@08MO-BW-813I-03M1", -1], ["E", 0]],
#     "energia_filtragem_igual": [["Calculo da Energia da Filtragem", 1], ["energia_filtro", -1], ["E", 0]],
#     "dosagem_antracito": [["antracito", 1], ["PESO1_I@08MO-BW-813I-03M1", -1], ["PESO1_I@08MO-BW-813I-04M1", -1], ["E", 0]],
#     "rotacao_rolos_mesa": [["rotacaoPeneiras2", 2], ["ROTA1_I@08PE-PN-851I-02-1", -1], ["ROTA1_I@08PE-PN-851I-02-2", -1], ["E", 0]],
#     "vazao_misturadores": [["vazaomisturadores", -1], ["VAZA1_I@08MI-MI-832I-01", 1], ["VAZA1_I@08MI-MI-832I-02", 1], ["E", 0]], 
#     "rotacao_rolos_mesa": [["rotacaoPeneiras2", 2], ["ROTA1_I@08PE-PN-851I-02-1", -1], ["ROTA1_I@08PE-PN-851I-02-2", -1], ["E", 0]],
#     "bombas_homogs": [["Soma_FUNC1_D_HO-BP-826I-05_08", 4], ["FUNC1_D@08HO-BP-826I-05M1", -1],
#                       ["FUNC1_D@08HO-BP-826I-06RM1", -1], ["FUNC1_D@08HO-BP-826I-07M", -1], ["FUNC1_D@08HO-BP-826I-08RM1", -1], ["E", 0]],
      
    "taxarp_limit_min": [["taxarp", 1], 0, ["GTE", 0]],
    "taxarp_limit_max": [["taxarp", 1], 0, ["LTE", 35]]
}