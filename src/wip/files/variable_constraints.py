variable_constraints = {
	"umidade_limit_min": [{"feature": "umidade", "coef": 1},
						  {"feature": "umidade", "limit": "lmin", "coef": -1},
						  {"operator": "GTE", "coef": 0}],
	"umidade_limit_max": [{"feature": "umidade", "coef": 1},
						  {"feature": "umidade", "limit": "lmax", "coef": -1},
						  {"operator": "LTE", "coef": 8}],
	"producao_min": [{"feature": "PROD_PQ_Y@08US", "coef": 1},
					 {"feature": "PROD_PQ_Y@08US", "limit": "fmin", "operator": "norm", "coef": -1},
					 {"operator": "GTE", "coef": 0}],
	"producao_max": [{"feature": "PROD_PQ_Y@08US", "coef": 1},
					 {"feature": "PROD_PQ_Y@08US", "limit": "fmax", "operator": "norm", "coef": -1},
					 {"operator": "LTE", "coef": 0}]
}