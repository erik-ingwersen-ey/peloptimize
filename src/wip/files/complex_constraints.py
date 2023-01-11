complex_constraints = {
  "GANHO_EQ": [
# 				 {"type": "complex", "feature": "SUP_SE_PP_L@08PR", "operator": "scaler", "position": "range", "coef": 1},
#                  {"type": "complex", "feature": "SUP_SE_PR_L@08FI", "operator": "scaler", "position": "range", "coef": -1},				 
# 				 {"operator": "E", "coef": 0},
				 {"type": "operation", "terms":
				 [{"type": "complex", "feature": "GANHO PRENSA - US8", "operator": "scaler", "position": "range", "coef": 1}]}],
  
# 	"Media_ROTA1_I_AD-BR-813I-01_02_equality": [{"type": "complex", "feature": "Media_ROTA1_I_AD-BR-813I-01_02", "operator": "scaler",
# 						 "position": "range", "coef": -1},
# 						 {"type": "complex", "feature": "ROTA1_I@08AD-BR-813I-{:02}M1", "start": 1, "end": 2,
# 						  "operator": "scaler", "position": "range", "coef": 0.5},
# 						 {"operator": "E", "coef": -1, "operation": "sum"},
# 						 {"type": "operation", "terms": 
# 						 	[{"type": "complex", "feature": "ROTA1_I@08AD-BR-813I-{:02}M1", "start": 1, "end": 2,
# 						  	"operator": "scaler", "position": "min", "coef": 0.5},
# 						  	{"type": "complex", "feature": "Media_ROTA1_I_AD-BR-813I-01_02", "operator": "scaler",
# 						 	"position": "min", "coef": -1}]}],
  
#   "Soma_PESO1_I_MO-BW-813I-03_04_equality": [{"type": "complex", "feature": "Soma_PESO1_I_MO-BW-813I-03_04", "operator": "scaler",
# 						 "position": "range", "coef": 1},
# 						 {"type": "complex", "feature": "PESO1_I@08MO-BW-813I-{:02}M1", "start": 3, "end": 4,
# 						  "operator": "scaler", "position": "range", "coef": -1},
# 						 {"operator": "E", "coef": -1, "operation": "sum"},
# 						 {"type": "operation", "terms": 
# 						 	[{"type": "complex", "feature": "PESO1_I@08MO-BW-813I-{:02}M1", "start": 3, "end": 4,
# 						  	"operator": "scaler", "position": "min", "coef": 1},
# 						  	{"type": "complex", "feature": "Soma_PESO1_I_MO-BW-813I-03_04", "operator": "scaler",
# 						 	"position": "min", "coef": -1}]}],      
  
# 	"Media_ROTA1_I_HO-AG-826I-01_03_equality": [{"type": "complex", "feature": "Media_ROTA1_I_HO-AG-826I-01_03", "operator": "scaler",
# 							"position": "range", "coef": -1},
# 						   {"type": "complex", "feature": "ROTA1_I@08HO-AG-826I-{:02}M1", "start": 1, "end": 3,
# 						  	"operator": "scaler", "position": "range", "coef": 0.333333333333},
# 						   {"operator": "E", "coef": -1, "operation": "sum"},
# 						   {"type": "operation", "terms":
# 							   [{"type": "complex", "feature": "ROTA1_I@08HO-AG-826I-{:02}M1", "start": 1, "end": 3,
# 							  	"operator": "scaler", "position": "min", "coef": 0.333333333333},
# 							   {"type": "complex", "feature": "Media_ROTA1_I_HO-AG-826I-01_03", "operator": "scaler",
# 								"position": "min", "coef": -1}]}],
  
# 	"Soma_PESO1_I_MO-BW-821I_01_03_equality": [{"type": "complex", "feature": "Soma_PESO1_I_MO-BW-821I_01_03", "operator": "scaler",
# 							 "position": "range", "coef": -1},
# 							{"type": "complex", "feature": "PESO1_I@08MO-BW-821I-{:02}M1", "start": 1, "end": 3,
# 							 "operator": "scaler", "position": "range", "coef": 1},
# 							{"operator": "E", "coef": -1, "operation": "sum"},
# 							{"type": "operation", "terms":
# 								[{"type": "complex", "feature": "PESO1_I@08MO-BW-821I-{:02}M1", "start": 1, "end": 3,
# 							 	 "operator": "scaler", "position": "min", "coef": 1},
# 								 {"type": "complex", "feature": "Soma_PESO1_I_MO-BW-821I_01_03", "operator": "scaler",
# 							 	 "position": "min", "coef": -1}]}],
  
# 	"Media_NIVE1_I_MO-TQ-821I-01_03":[{"type": "complex", "feature": "Media_NIVE1_I_MO-TQ-821I-01_03", "operator": "scaler",
# 							 "position": "range", "coef": -1},
# 							{"type": "complex", "feature": "NIVE1_I@08MO-TQ-821I-{:02}", "operator": "scaler",
# 							 "position": "range", "coef": 0.3333333333333333333333333, "start": 1, "end": 3},
# 							{"operator": "E", "coef": -1, "operation": "sum"},
# 							{"type": "operation", "terms":
# 								[{"type": "complex", "feature": "Media_NIVE1_I_MO-TQ-821I-01_03", "operator": "scaler",
# 								  "position": "min", "coef": -1},
# 								{"type": "complex", "feature": "NIVE1_I@08MO-TQ-821I-{:02}", "operator": "scaler",
# 							 	"position": "min", "coef": 0.3333333333333333333333333, "start": 1, "end": 3}]}],

  
    
#       "Media_NIVE1_I_HO-TQ-826I-03_05_equality": [{"type": "complex", "feature": "Media_NIVE1_I_HO-TQ-826I-03_05", "operator": "scaler",
#                          "position": "range", "coef": 3},
#                         {"type": "complex", "feature": "NIVE1_I@08HO-TQ-826I-{:02}", "start": 3, "end": 5,
#                          "operator": "scaler", "position": "range", "coef": -1},
#                         {"operator": "E", "coef": -1, "operation": "sum"},
#                         {"type": "operation", "terms":
#                             [{"type": "complex", "feature": "NIVE1_I@08HO-TQ-826I-{:02}", "start": 3, "end": 5,
#                              "operator": "scaler", "position": "min", "coef": -1},
#                              {"type": "complex", "feature": "Media_NIVE1_I_HO-TQ-826I-03_05", "operator": "scaler",
#                              "position": "min", "coef": 3}]}],
  

#     "Soma_PESO1_I_MO-BW-813I-03_04_equality": [{"type": "complex", "feature": "Soma_PESO1_I_MO-BW-813I-03_04", "operator": "scaler",
#                          "position": "range", "coef": 1},
#                         {"type": "complex", "feature": "PESO1_I@08MO-BW-813I-{:02}M1", "start": 3, "end": 4,
#                          "operator": "scaler", "position": "range", "coef": -1},
#                         {"operator": "E", "coef": -1, "operation": "sum"},
#                         {"type": "operation", "terms":
#                             [{"type": "complex", "feature": "PESO1_I@08MO-BW-813I-{:02}M1", "start": 3, "end": 4,
#                              "operator": "scaler", "position": "min", "coef": 1},
#                              {"type": "complex", "feature": "Soma_PESO1_I_MO-BW-813I-03_04", "operator": "scaler",
#                              "position": "min", "coef": -1}]}],
  
  
#     "Soma_PESO1_I_MO-BW-813I-03_04_equality": [{"type": "complex", "feature": "Soma_FUNC1_D_HO-BP-826I-05_08", "operator": "scaler",
#                          "position": "range", "coef": 1},
#                         {"type": "complex", "feature": "FUNC1_D@08HO-BP-826I-05M1", "coef": -1},
#                         {"type": "complex", "feature": "FUNC1_D@08HO-BP-826I-06RM1", "coef": -1},
#                         {"type": "complex", "feature": "FUNC1_D@08HO-BP-826I-07M1","coef": -1},
#                         {"type": "complex", "feature": "FUNC1_D@08HO-BP-826I-08RM1", "coef": -1},
#                         {"operator": "E", "coef": -1, "operation": "sum"},
#                         {"type": "operation", "terms":
#                             [{"type": "complex", "feature": "FUNC1_D@08HO-BP-826I-05M1", "coef": 1},
#                              {"type": "complex", "feature": "FUNC1_D@08HO-BP-826I-06RM1", "coef": 1},
#                              {"type": "complex", "feature": "FUNC1_D@08HO-BP-826I-07M1", "coef": 1},
#                              {"type": "complex", "feature": "FUNC1_D@08HO-BP-826I-08RM1", "coef": 1},
#                              {"type": "complex", "feature": "Soma_FUNC1_D_HO-BP-826I-05_08", "operator": "scaler",
#                              "position": "min", "coef": -1}]}],
  
#   	"Soma_PESO1_I_PE-BW-840I-01_12_equality": [{"type": "complex", "feature": "Soma_PESO1_I_PE-BW-840I-01_12", "operator": "scaler",
# 							 "position": "range", "coef": -1},
# 							{"type": "complex", "feature": "PESO1_I@08PE-BW-840I-{:02}M1", "start": 1, "end": 12,
# 							 "operator": "scaler", "position": "range", "coef": 1},
# 							{"operator": "E", "coef": -1, "operation": "sum"},
# 							{"type": "operation", "terms":
# 								[{"type": "complex", "feature": "PESO1_I@08PE-BW-840I-{:02}M1", "start": 1, "end": 12,
# 							 	 "operator": "scaler", "position": "min", "coef": 1},
# 								 {"type": "complex", "feature": "Soma_PESO1_I_PE-BW-840I-01_12", "operator": "scaler",
# 							 	 "position": "min", "coef": -1}]}],   
  
#   "Media_ROTA1_I_PE-BD-840I-01_12_calc_equality": [{"type": "complex", "feature": "Media_ROTA1_I_PE-BD-840I-01_12", "operator": "scaler",
# 							"position": "range", "coef": 12},
# 						   {"type": "complex", "feature": "rota_disco_{}", "start": 1, "end": 12, "coef": -1},
# 						   {"operator": "E", "coef": 1, "operation": "sum"},
# 						   {"type": "operation", "terms":
# 							   [{"type": "complex", "feature": "Media_ROTA1_I_PE-BD-840I-01_12", "operator": "scaler",
# 								"position": "min", "coef": -12}]}],
  
  
#  "Media_ROTA1_I_PE-PN-840I-01_12_calc_equality": [{"type": "complex", "feature": "Media_ROTA1_I_PE-PN-840I-01_12", "operator": "scaler",
#                           "position": "range", "coef": 12},
#                          {"type": "complex", "feature": "ROTA1_I@08PE-PN-840I-{:02}M1", "start": 1, "end": 12, "coef": -1, "operator": "scaler", "position":"range"},
#                          {"operator": "E", "coef": 1, "operation": "sum"},
#                          {"type": "operation", "terms":
#                              [{"type": "complex", "feature": "Media_ROTA1_I_PE-PN-840I-01_12", "operator": "scaler",
#                               "position": "min", "coef": -12},
#                               {"type": "complex", "feature": "ROTA1_I@08PE-PN-840I-{:02}M1", "start": 1, "end": 12, "operator": "scaler",
#                               "position": "min", "coef": 1}]}],
  
# "Soma_VAZA1_I_MI-MI-832I-01_02_calc_equality": [{"type": "complex", "feature": "Soma_VAZA1_I_MI-MI-832I-01_02", "operator": "scaler",
#                          "position": "range", "coef": -1},
#                         {"type": "complex", "feature": "VAZA1_I@08MI-MI-832I-{:02}", "start": 1, "end": 2,
#                          "operator": "scaler", "position": "range", "coef": 1},
#                         {"operator": "E", "coef": -1, "operation": "sum"},
#                         {"type": "operation", "terms":
#                             [{"type": "complex", "feature": "VAZA1_I@08MI-MI-832I-{:02}", "start": 1, "end": 2,
#                              "operator": "scaler", "position": "min", "coef": 1},
#                              {"type": "complex", "feature": "Soma_VAZA1_I_MI-MI-832I-01_02", "operator": "scaler",
#                              "position": "min", "coef": -1}]}], 
  
  
# "Media_ROTA1_I_PE-PN-851I-02-1_2_calc_equality": [{"type": "complex", "feature": "Media_ROTA1_I_PE-PN-851I-02-1_2", "operator": "scaler",
#                         "position": "range", "coef": 2},
#                        {"type": "complex", "feature": "ROTA1_I@08PE-PN-851I-02-{}", "start": 1, "end": 2, "coef": -1, "operator": "scaler", "position": "range"},
#                        {"operator": "E", "coef": 1, "operation": "sum"},
#                        {"type": "operation", "terms":
#                            [{"type": "complex", "feature": "Media_ROTA1_I_PE-PN-851I-02-1_2", "operator": "scaler",
#                             "position": "min", "coef": -2},
#                            {"type": "complex", "feature": "ROTA1_I@08PE-PN-851I-02-{}", "operator": "scaler", "start": 1, "end": 2,
#                             "position": "min", "coef": 1}
#                         ]}],
  
# "Media_GRAN_OCS_TM@08PE-BD-840I-1_12_calc_equality": [{"type": "complex", "feature": "Media_GRAN_OCS_TM_PE-BD-840I-01_12", "operator": "scaler",
#                       "position": "range", "coef": 12},
#                      {"type": "complex", "feature": "GRAN_OCS_TM@08PE-BD-840I-{:02}", "start": 1, "end": 12, "coef": -1, "operator": "scaler", "position": "range"},
#                      {"operator": "E", "coef": 1, "operation": "sum"},
#                      {"type": "operation", "terms":
#                          [{"type": "complex", "feature": "Media_GRAN_OCS_TM_PE-BD-840I-01_12", "operator": "scaler",
#                           "position": "min", "coef": -12},
#                          {"type": "complex", "feature": "GRAN_OCS_TM@08PE-BD-840I-{:02}", "operator": "scaler", "start": 1, "end": 12,
#                           "position": "min", "coef": 1}
#                       ]}],
  
# "Soma_FUNC1_D@08MO-BW-821I-01_03_equality": [{"type": "complex", "feature": "Soma_FUNC1_D@08MO-BW-821I-01_03", "operator": "scaler", "coef": 1},
#                      {"type": "complex", "feature": "FUNC1_D@08MO-BW-821I-{:02}M1", "start": 1, "end": 3, "coef": -1},
#                      {"operator": "E", "coef": -1, "operation": "sum"},
#                      {"type": "operation", "terms": 
#                         [{"type": "complex", "feature": "FUNC1_D@08MO-BW-821I-{:02}M1", "start": 1, "end": 3, "coef": 1},
#                         {"type": "complex", "feature": "Soma_FUNC1_D@08MO-BW-821I-01_03", "coef": -1}]}],
  
# "Media_DENS1_C_HO-BP-826I-05_08_calc_equality" : [{"type": "complex", "feature": "Media_DENS1_C_HO-BP-826I-05_08", "operator": "scaler",
#                         "position": "range", "coef": 4},
#                        {"type": "complex", "feature": "DENS1_C@08HO-BP-826I-05", "coef": -1, "operator": "scaler", "position": "range"},
#                        {"type": "complex", "feature": "DENS1_C@08HO-BP-826I-06R", "coef": -1, "operator": "scaler", "position": "range"},
#                        {"type": "complex", "feature": "DENS1_C@08HO-BP-826I-07", "coef": -1, "operator": "scaler", "position": "range"},
#                        {"type": "complex", "feature": "DENS1_C@08HO-BP-826I-08R", "coef": -1, "operator": "scaler", "position": "range"},
#                        {"operator": "E", "coef": 1, "operation": "sum"},
#                        {"type": "operation", "terms":
#                            [{"type": "complex", "feature": "Media_DENS1_C_HO-BP-826I-05_08", "operator": "scaler",
#                             "position": "min", "coef": -4},
#                            {"type": "complex", "feature": "DENS1_C@08HO-BP-826I-05", "operator": "scaler",
#                             "position": "min", "coef": 1},
#                            {"type": "complex", "feature": "DENS1_C@08HO-BP-826I-06R", "operator": "scaler",
#                             "position": "min", "coef": 1},
#                            {"type": "complex", "feature": "DENS1_C@08HO-BP-826I-07", "operator": "scaler",
#                             "position": "min", "coef": 1},
#                            {"type": "complex", "feature": "DENS1_C@08HO-BP-826I-08R", "operator": "scaler",
#                             "position": "min", "coef": 1}
#                         ]}],
  
#   "Media_NIVE1_6_QU-FR-851I-01M1_equality": [{"type": "complex", "feature": "Media_NIVE1_6_QU-FR-851I-01M1", "operator": "scaler",
#                    "position": "range", "coef": 6},
#                   {"type": "complex", "feature": "NIVE{:01}_I@08QU-FR-851I-01M1", "start": 2, "end": 6,
#                    "operator": "scaler", "position": "range", "coef": -1},
#                   {"type": "complex", "feature": "NIVE1_C@08QU-FR-851I-01M1",
#                    "operator": "scaler", "position": "range", "coef": -1},
#                   {"operator": "E", "coef": -1, "operation": "sum"},
#                   {"type": "operation", "terms":
#                       [{"type": "complex", "feature": "NIVE{:01}_I@08QU-FR-851I-01M1", "start": 2, "end": 6,
#                        "operator": "scaler", "position": "min", "coef": -1},
#                        {"type": "complex", "feature": "NIVE1_C@08QU-FR-851I-01M1","operator": "scaler", "position": "min", "coef": -1},
#                        {"type": "complex", "feature": "Media_NIVE1_6_QU-FR-851I-01M1", "operator": "scaler",
#                        "position": "min", "coef": 6}]}],
  
  
#   "vm_balsa_calc_equality_equality": [{"type": "complex", "feature": "VM Balsa", "operator": "scaler",
#                      "position": "range", "coef": -1},
#                     {"type": "complex", "feature": "VAZA1_I@08AP-BP-875I-01",
#                      "operator": "scaler", "position": "range", "coef": 1},
#                     {"type": "complex", "feature": "DENS1_I@08AP-TQ-875I-03",
#                      "operator": "scaler", "position": "range", "coef": 1},
#                     {"operator": "E", "coef": -1, "operation": "sum"},
#                     {"type": "operation", "terms":
#                         [{"type": "complex", "feature": "VAZA1_I@08AP-BP-875I-01",
#                          "operator": "scaler", "position": "min", "coef": 1},
#                          {"type": "complex", "feature": "DENS1_I@08AP-TQ-875I-03",
#                          "operator": "scaler", "position": "min", "coef": 1},
#                          {"type": "complex", "feature": "Soma_PESO1_I_MO-BW-813I-03_04", "operator": "scaler",
#                          "position": "min", "coef": -1}]}],
  

	
# 	"rotacao_funcionamento": [
# 						{"type": "complex", "feature": "ROTA1_I@08FI-FL-827I-{:02}M1", "start": 1, "end": 4,
# 						 "operator": "scaler", "position": "range", "coef": 1},
# 						{"type": "complex", "feature": "ROTA1_I@08FI-FL-827I-{:02}M1", "start": 6, "end": 9,
# 						 "operator": "scaler", "position": "range", "coef": 1},
# 						{"type": "complex", "feature": "ROTA1_I@08FI-FL-827I-05RM1", "operator": "scaler",
# 						 "position": "range", "coef": 1},
# 						{"type": "complex", "feature": "ROTA1_I@08FI-FL-827I-10RM1", "operator": "scaler",
# 						 "position": "range", "coef": 1},
# 						{"operator": "E", "coef": 1, "operation": "sum"},
# 						{"type": "operation", "terms":
# 							[{"feature": "FUNC1_D@08FI-FL-827I-{:02}", "start": 1, "end": 4, "coef": 0.8},
#                              {"feature": "FUNC1_D@08FI-FL-827I-{:02}", "start": 6, "end": 9, "coef": 0.8},
#                              {"feature": "FUNC1_D@08FI-FL-827I-{:02}R", "start": 5,"end": 10, "step": 5, "coef": 0.8}]}],

 
#  "taxa_alimentacao_filtros_vs_producao": [{"feature": "FUNC1_D@08FI-FL-827I-{:02}", "start": 1, "end": 4, "coef": 105},											 
# 											 {"feature": "FUNC1_D@08FI-FL-827I-{:02}R", "start": 5, "end": 5, "coef": 105},		
#                                           {"feature": "FUNC1_D@08FI-FL-827I-{:02}", "start": 6, "end": 9, "coef": 105},
#                                           {"feature": "FUNC1_D@08FI-FL-827I-{:02}R", "start": 10, "end": 10, "coef": 105},
# 											 {"operator": "GTE", "coef": 1, "operation": None},
# 											 {"type": "operation", "terms": 
# 											  [{"type": "complex", "feature": "PROD_PQ_Y@08US", "operator": "scaler", "position": "range", "coef": 0.86}]}],
  
#  "taxa_alimentacao_filtros_vs_producao_L1": [{"feature": "FUNC1_D@08FI-FL-827I-{:02}", "start": 1, "end": 4, "coef": 129},											 
# 											 {"feature": "FUNC1_D@08FI-FL-827I-{:02}R", "start": 5, "end": 5, "coef": 129},											                                              
# 											 {"operator": "GTE", "coef": 1, "operation": None},
# 											 {"type": "operation", "terms": 
# 											  [{"type": "complex", "feature": "PROD_PQ_Y@08US", "operator": "scaler", "position": "range", "coef": 0.86}]}],

#  "taxa_alimentacao_filtros_vs_producao_L2": [{"feature": "FUNC1_D@08FI-FL-827I-{:02}", "start": 6, "end": 9, "coef": 129},
# 											 {"feature": "FUNC1_D@08FI-FL-827I-{:02}R", "start": 10, "end": 10, "coef": 129},											                                              
# 											 {"operator": "GTE", "coef": 1, "operation": None},
# 											 {"type": "operation", "terms": 
# 											  [{"type": "complex", "feature": "PROD_PQ_Y@08US", "operator": "scaler", "position": "range", "coef": 0.86}]}],
  
# "taxa_alimentacao_bv_vs_producao_L1": [{"feature": "FUNC1_D@08FI-BV-827I-{:02}M1", "start": 1, "end": 4, "coef": 129},											 
# 											 {"feature": "FUNC1_D@08FI-BV-827I-{:02}RM1", "start": 5, "end": 5, "coef": 129},											                                              
# 											 {"operator": "GTE", "coef": 1, "operation": None},
# 											 {"type": "operation", "terms": 
# 											  [{"type": "complex", "feature": "PROD_PQ_Y@08US", "operator": "scaler", "position": "range", "coef": 0.86}]}],

#  "taxa_alimentacao_bv_vs_producao_L2": [{"feature": "FUNC1_D@08FI-BV-827I-{:02}M1", "start": 6, "end": 9, "coef": 129},
# 											 {"feature": "FUNC1_D@08FI-BV-827I-{:02}RM1", "start": 10, "end": 10, "coef": 129},											                                              
# 											 {"operator": "GTE", "coef": 1, "operation": None},
# 											 {"type": "operation", "terms": 
# 											  [{"type": "complex", "feature": "PROD_PQ_Y@08US", "operator": "scaler", "position": "range", "coef": 0.86}]}],

#   "taxa_alimentacao_filtros_vs_producao": [{"feature": "FUNC1_D@08FI-FL-827I-{:02}", "start": 1, "end": 4, "coef": 157.716},
# 											 {"feature": "FUNC1_D@08FI-FL-827I-{:02}", "start": 6, "end": 9, "coef": 157.716},
# 											 {"feature": "FUNC1_D@08FI-FL-827I-{:02}R", "start": 5,"end": 10, "step": 5, "coef": 157.716},
# 											 {"type": "complex", "feature": "PROD_PQ_Y@08US", "operator": "scaler", "position": "range", "coef": -1},
# 											 {"operator": "GTE", "coef": 1, "operation": None},
# 											 {"type": "operation", "terms": 
# 											  [{"type": "complex", "feature": "PROD_PQ_Y@08US", "operator": "scaler",
# 											  	"position": "min", "coef": 1}]}],
  
#   "taxa_alimentacao_bv_vs_producao": [{"feature": "FUNC1_D@08FI-BV-827I-{:02}M1", "start": 1, "end": 4, "coef": 137},
# 											 {"feature": "FUNC1_D@08FI-BV-827I-{:02}M1", "start": 6, "end": 9, "coef": 137},
# 											 {"feature": "FUNC1_D@08FI-BV-827I-{:02}RM1", "start": 5,"end": 10, "step": 5, "coef": 137},
# 											 {"type": "complex", "feature": "PROD_PQ_Y@08US", "operator": "scaler", "position": "range", "coef": -1},
# 											 {"operator": "GTE", "coef": 1, "operation": None},
# 											 {"type": "operation", "terms": 
# 											  [{"type": "complex", "feature": "PROD_PQ_Y@08US", "operator": "scaler",
# 											  	"position": "min", "coef": 1}]}],

	"taxa_alimentacao_discos_vs_producao": [{"type": "complex", "feature": "PESO1_I@08PE-BW-840I-{:02}M1",
											 "start": 1, "end": 12, "coef": 0.5914171520869509, "operator": "scaler",
											 "position": "range"},
											{"type": "complex", "feature": "PROD_PQ_Y@08US", "operator": "scaler",
											  "position": "range", "coef": -1},
											{"operator": "E", "coef": 1, "operation": "sum"},
											{"type": "operation", "terms":
                                             
											 [{"type": "complex", "feature": "PESO1_I@08PE-BW-840I-{:02}M1", "start": 1,
											  "end": 12, "operator": "scaler", "position": "min", "coef": -0.5914171520869509},
											  {"type": "complex", "feature": "PROD_PQ_Y@08US", "operator": "scaler",
											   "position": "min", "coef": 1}]}],
  
  
#   'energia_filtragem_igual': [{"type": "complex", "feature": "Calculo da Energia da Filtragem", "coef": -1},
# 						 {"type": "complex", "feature": "energia_filtro", "coef":1},
# 						 {"operator": "E", "coef": 0,},
# 						]
}


