range_complex_constraints = {
	"taxa_alimentacao_disco_min_{:02}": [{"type": "range", "feature": "PESO1_I@08PE-BW-840I-{:02}M1",
										  "start": 1, "end": 12, "coef": 1},
										 {"type": "range", "feature": "FUNC1_D@08PE-BD-840I-{:02}M1",
										  "start": 1, "end": 12, "operator": "norm", "coef": -1,
										  "norm_feature": "PESO1_I@08PE-BW-840I-{:02}M1", "limit": 90},
										 {"operator": "GTE", "coef": 0}],
	"taxa_alimentacao_disco_max_{:02}": [{"type": "range", "feature": "PESO1_I@08PE-BW-840I-{:02}M1",
										  "start": 1, "end": 12, "coef": 1},
										 {"type": "range", "feature": "FUNC1_D@08PE-BD-840I-{:02}M1",
										  "start": 1, "end": 12, "operator": "norm", "coef": -1,
										  "norm_feature": "PESO1_I@08PE-BW-840I-{:02}M1", "limit": 140},
										 {"operator": "LTE", "coef": 0}],
	"rotacao_disco_min_{:02}": [{"type": "range", "feature": "rota_disco_{}", "start": 1, "end": 12, "coef": 1},
								{"type": "range", "feature": "FUNC1_D@08PE-BD-840I-{:02}M1", "start": 1, "end": 12, "coef": -5},
								{"operator": "GTE", "coef": 0}],
	"rotacao_disco_max_{:02}": [{"type": "range", "feature": "rota_disco_{}", "start": 1, "end": 12, "coef": 1},
								{"type": "range", "feature": "FUNC1_D@08PE-BD-840I-{:02}M1", "start": 1, "end": 12, "coef": -6.7},
								{"operator": "LTE", "coef": 0}],
	"tm_disco_min_{:02}": [{"type": "range", "feature": "GRAN_OCS_TM@08PE-BD-840I-{:02}", "start": 1, "end": 12, "coef": 1},
						   {"type": "range", "feature": "FUNC1_D@08PE-BD-840I-{:02}M1", "start": 1, "end": 12, "coef": -1,
						    "operator": "norm", "limit": 12, "norm_feature": "GRAN_OCS_TM@08PE-BD-840I-{:02}"},
						   {"operator": "GTE", "coef": 0}],
	"tm_disco_max_{:02}": [{"type": "range", "feature": "GRAN_OCS_TM@08PE-BD-840I-{:02}", "start": 1, "end": 12, "coef": 1},
						   {"type": "range", "feature": "FUNC1_D@08PE-BD-840I-{:02}M1", "start": 1, "end": 12, "coef": -1,
						    "operator": "norm", "limit": 14, "norm_feature": "GRAN_OCS_TM@08PE-BD-840I-{:02}"},
						   {"operator": "LTE", "coef": 0}],
	"potencia_moinho_min_{:02}": [{"type": "range", "feature": "POTE1_I@08MO-MO-821I-{:02}M1", "start": 1, "end": 3, "coef": 1},
								  {"type": "range", "feature": "FUNC1_D@08MO-MO-821I-{:02}M1", "start": 1, "end": 3, "coef": -1,
								   "operator": "norm", "limit": 5.48, "norm_feature": "POTE1_I@08MO-MO-821I-{:02}M1"},
								  {"operator": "GTE", "coef": 0}],
	"potencia_moinho_max_{:02}": [{"type": "range", "feature": "POTE1_I@08MO-MO-821I-{:02}M1", "start": 1, "end": 3, "coef": 1},
								  {"type": "range", "feature": "FUNC1_D@08MO-MO-821I-{:02}M1", "start": 1, "end": 3, "coef": -1,
								   "operator": "norm", "limit": 6, "norm_feature": "POTE1_I@08MO-MO-821I-{:02}M1"},
								  {"operator": "LTE", "coef": 0}],
	"alim_moinho_min_{:02}": [{"type": "range", "feature": "PESO1_I@08MO-BW-821I-{:02}M1", "start": 1, "end": 3, "coef": 1},
								{"type": "range", "feature": "FUNC1_D@08MO-MO-821I-{:02}M1", "start": 1, "end": 3, "coef": -1,
								 "operator": "norm", "limit": 310, "norm_feature": "PESO1_I@08MO-BW-821I-{:02}M1"},
								{"operator": "GTE", "coef": 0}],
	"alim_moinho_max_{:02}": [{"type": "range", "feature": "PESO1_I@08MO-BW-821I-{:02}M1", "start": 1, "end": 3, "coef": 1},
								{"type": "range", "feature": "FUNC1_D@08MO-MO-821I-{:02}M1", "start": 1, "end": 3, "coef": -1,
								 "operator": "norm", "limit": 490, "norm_feature": "PESO1_I@08MO-BW-821I-{:02}M1"},
								{"operator": "LTE", "coef": 0}],
  "equality_bv_filtro_{:02}_1_4": [{"type": "range", "feature": "FUNC1_D@08FI-FL-827I-{:02}", "start": 1, "end": 4, "coef": -1},
                                   {"type": "range", "feature": "FUNC1_D@08FI-BV-827I-{:02}M1", "start": 1, "end": 4, "coef": 1},								
								{"operator": "E", "coef": 0}],
  "equality_bv_filtro_{:02}_6_9": [{"type": "range", "feature": "FUNC1_D@08FI-FL-827I-{:02}", "start": 6, "end": 9, "coef": -1},
                                   {"type": "range", "feature": "FUNC1_D@08FI-BV-827I-{:02}M1", "start": 6, "end": 9, "coef": 1},
								{"operator": "E", "coef": 0}],
  "equality_bv_filtro_{:02}_R": [{"type": "range", "feature": "FUNC1_D@08FI-FL-827I-{:02}R", "start": 5, "end": 10, "step": 5, "coef": -1},
                                 {"type": "range", "feature": "FUNC1_D@08FI-BV-827I-{:02}RM1", "start": 5, "end": 10, "step": 5, "coef": 1},
								{"operator": "E", "coef": 0}]
  
#   "rot_fi_fl{:02}_1_4": [{"type": "range", "feature": "ROTA1_I@08FI-FL-827I-{:02}M1", "start": 1, "end": 4, "coef": 1},
# 								{"type": "range", "feature": "FUNC1_D@08FI-FL-827I-{:02}", "start": 1, "end": 4, "coef": -1,
# 								 "operator": "norm", "limit": 0.8, "norm_feature": "ROTA1_I@08FI-FL-827I-{:02}M1"},
# 								{"operator": "GTE", "coef": 0}],
#   "rot_fi_fl{:02}_6_9": [{"type": "range", "feature": "ROTA1_I@08FI-FL-827I-{:02}M1", "start": 6, "end": 9, "coef": 1},
# 								{"type": "range", "feature": "FUNC1_D@08FI-FL-827I-{:02}", "start": 6, "end": 9, "coef": -1,
# 								 "operator": "norm", "limit": 0.8, "norm_feature": "ROTA1_I@08FI-FL-827I-{:02}M1"},
# 								{"operator": "GTE", "coef": 0}],
#   "rot_fi_fl{:02}_R": [{"type": "range", "feature": "ROTA1_I@08FI-FL-827I-{:02}RM1", "start": 5, "end": 10, "step": 5, "coef": 1},
# 								{"type": "range", "feature": "FUNC1_D@08FI-FL-827I-{:02}R", "start": 5, "end": 10, "step": 5, "coef": -1,
# 								 "operator": "norm", "limit": 0.8, "norm_feature": "ROTA1_I@08FI-FL-827I-{:02}RM1"},
# 								{"operator": "GTE", "coef": 0}]
}