""" Write output files. """

import os
import re

import statistics
import pandas as pd
from scipy import stats as s
import numpy as np

import importlib

def write_objective_function_coef(solver_path, scalers):
    real_cost = operations.read_json(f"{solver_path}/custo_real.json")

    tokens = solver_operations.define_range_constraints("energia_moinho", 1, 2)
    real_cost = solver_operations.adjust_real_cost(real_cost, tokens)

    tokens = solver_operations.define_range_constraints("corpo_moedor_especifico_{}", 1, 4)
    real_cost = solver_operations.adjust_real_cost(real_cost, tokens, 1, 1)
    
    opt_costs = solver_operations.scale_optimization_tags(scalers, real_cost)

    obj_coefs = pd.DataFrame.from_dict(opt_costs, orient='index', columns=['Custo'])

    obj_coefs.index.name = "TAG"

    obj_coefs = obj_coefs.loc[constants.OBJ_FUNC_COEF]

    obj_coefs = obj_coefs.to_csv(os.path.join(solver_path, "costs.csv"), decimal=',', sep=';')   
    
    return obj_coefs


def tags_equality(solver_path, scalers, datasets):
    custos_reais = pd.read_csv(f"{solver_path}/costs.csv", decimal=',', sep=';', index_col='TAG').to_dict().get('Custo')
    custos_reais = solver_operations.unnormalize_optimization_tags(scalers, custos_reais)

    results_otm = []
    inv_tag_2_var = {v: k for k, v in constants.TARGETS_IN_MODEL.items()}
    columns = ['faixa', 'TAG', 'minimo', 'maximo', 'valor normalizado', 'valor real', 'custo']
    df_output = pd.DataFrame(columns=columns)
    
    # INICIO: pos-processamento para garantir que o DENS1_C@08HO-BP-826I seja igual para todas as faixas
    dens = []
    gran_ocs = []
    rota_fi = {'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]}

    for fmin, fmax in constants.production_range:
            f = 'Variables - VarX_{}-{}.csv'.format(fmin, fmax)

            if not os.path.exists(os.path.join(solver_path, f)):                    
                    constants.production_range.remove((fmin, fmax))
                    continue
                    
            df = pd.read_csv(os.path.join(solver_path, f), sep=';', encoding='iso-8859-1')
            
            for line in df.iterrows():

                    var_csv = line[1][' VariableName']
                    var = var_csv
                    coef_real = coef = line[1][' Value']
                    obj_coef_readl = obj_coef = line[1][' ObjCoeff']                                                    
                                        
                    if 'DENS1_C@08HO-BP-826I' not in var and 'GRAN_OCS_TM@08PE-BD-840I' not in var and 'ROTA1_I@08FI-FL-827I' not in var:
                            continue
                    
                    obj_coef_real =    operations.unnormalize_feature(scalers, var, obj_coef, operation='one feature')
                    coef_real =    operations.unnormalize_feature(scalers, var, coef, operation='one feature')                                            
                    
                    if 'DENS1_C@08HO-BP-826I' in var:
                            dens.append(coef_real)
#                             log.debug(f'DEBUG:  {var}, {coef_real}, {dens}')
                    elif 'GRAN_OCS_TM@08PE-BD-840I' in var:
#                             log.debug(f'DEBUG:  {var}, {coef_real}')
                            gran_ocs.append(coef_real)
#                             log.debug(f'DEBUG:  {gran_ocs}')
                    elif 'ROTA1_I@08FI-FL-827I' in var:
                            rota_fi[str(fmax)].append(coef_real)
    # alteraçao gabi de > para >= 20220510
    dens = [x for x in dens if float(x) >= 2.15]
    
#     log.info(f'{dens}')
    dens = s.mode(dens)[0][0]
#     dens = str(dens)
    gran_ocs = [x for x in gran_ocs if float(x) > 0] 
#     log.debug(f'DEBUG:  {gran_ocs}')
#     gran_ocs = statistics.mode(gran_ocs)
#     gran_ocs = s.mode(gran_ocs)
#     gran_ocs = gran_ocs[0][0]
#     log.debug(f'DEBUG2: {gran_ocs}')
            
    # FIM: pos-processamento para garantir que o DENS1_C seja igual para todas as faixas
    
    return dens, gran_ocs, rota_fi

def rot_pot_filtragem(solver_path, scalers, datasets):
    custos_reais = pd.read_csv(f"{solver_path}/costs.csv", decimal=',', sep=';', index_col='TAG').to_dict().get('Custo')
    custos_reais = solver_operations.unnormalize_optimization_tags(scalers, custos_reais)

    results_otm = []
    inv_tag_2_var = {v: k for k, v in constants.TARGETS_IN_MODEL.items()}
    columns = ['faixa', 'TAG', 'minimo', 'maximo', 'valor normalizado', 'valor real', 'custo']
    df_output = pd.DataFrame(columns=columns)    
    
    rot_func = {'ROTA1_I@08FI-FL-827I-01M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'ROTA1_I@08FI-FL-827I-02M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'ROTA1_I@08FI-FL-827I-03M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'ROTA1_I@08FI-FL-827I-04M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'ROTA1_I@08FI-FL-827I-05RM1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'ROTA1_I@08FI-FL-827I-06M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'ROTA1_I@08FI-FL-827I-07M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'ROTA1_I@08FI-FL-827I-08M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'ROTA1_I@08FI-FL-827I-09M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'ROTA1_I@08FI-FL-827I-10RM1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]}}
    
    pot_func = {'POTE1_I@08FI-BV-827I-01M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'POTE1_I@08FI-BV-827I-02M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'POTE1_I@08FI-BV-827I-03M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'POTE1_I@08FI-BV-827I-04M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'POTE1_I@08FI-BV-827I-05RM1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'POTE1_I@08FI-BV-827I-06M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'POTE1_I@08FI-BV-827I-07M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'POTE1_I@08FI-BV-827I-08M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'POTE1_I@08FI-BV-827I-09M1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]},'POTE1_I@08FI-BV-827I-10RM1':{'750':[],'800':[],'850':[],'900':[],'950':[],'1000':[]}}
    
    # INICIO: pos-processamento para garantir que o ROTA1_I@08FI-FL-827I seja igual ao FUNC * max_rot
    for fmin, fmax in constants.production_range:
            f = 'Variables - VarX_{}-{}.csv'.format(fmin, fmax)

            if not os.path.exists(os.path.join(solver_path, f)):                    
                    continue
            df = pd.read_csv(os.path.join(solver_path, f), sep=';', encoding='iso-8859-1')
            
            for line in df.iterrows():
                    var_csv = line[1][' VariableName']
                    var = var_csv
                    coef_real = coef = line[1][' Value']
                    obj_coef_readl = obj_coef = line[1][' ObjCoeff']                                                    
                                        
#                     if 'FUNC1_D@08FI-FL-827I' not in var or 'FUNC1_D@08FI-BV-827I' not in var:
#                             continue
                    
#                     if 'FUNC1_D' not in var:
                    if 'FUNC1_D@08FI-FL-827I' not in var and 'FUNC1_D@08FI-BV-827I' not in var:
                            continue
                    
                    if var in scalers:
                        obj_coef_real =    operations.unnormalize_feature(scalers, var, obj_coef, operation='one feature')
                        coef_real =    operations.unnormalize_feature(scalers, var, coef, operation='one feature')                                            
                    
                    if 'FUNC1_D@08FI-FL-827I' in var:
                            svar = var.replace('FUNC1_D', 'ROTA1_I') + "M1"
                            rot_func[svar][str(fmax)] = coef_real
                            
                    if 'FUNC1_D@08FI-BV-827I' in var:
                            svar = var.replace('FUNC1_D', 'POTE1_I')
                            pot_func[svar][str(fmax)] = coef_real     
                            
    return rot_func, pot_func
    # FIM: pos-processamento para garantir que o ROTA1_I@08FI-FL-827I seja igual ao FUNC * max_rot

def increasing_temp_gq(solver_path, scalers, datasets):
    custos_reais = pd.read_csv(f"{solver_path}/costs.csv", decimal=',', sep=';', index_col='TAG').to_dict().get('Custo')
    custos_reais = solver_operations.unnormalize_optimization_tags(scalers, custos_reais)

    results_otm = []
    inv_tag_2_var = {v: k for k, v in constants.TARGETS_IN_MODEL.items()}
    columns = ['faixa', 'TAG', 'minimo', 'maximo', 'valor normalizado', 'valor real', 'custo']
    df_output = pd.DataFrame(columns=columns)
    
    rot_func = {'TEMP1_I@08QU-QU-855I-GQ03':[],'TEMP1_I@08QU-QU-855I-GQ04':[],'TEMP1_I@08QU-QU-855I-GQ05':[],'TEMP1_I@08QU-QU-855I-GQ06':[],'TEMP1_I@08QU-QU-855I-GQ07':[],'TEMP1_I@08QU-QU-855I-GQ08':[],'TEMP1_I@08QU-QU-855I-GQ09':[],'TEMP1_I@08QU-QU-855I-GQ10':[],'TEMP1_I@08QU-QU-855I-GQ11':[],'TEMP1_I@08QU-QU-855I-GQ12':[], 'TEMP1_I@08QU-QU-855I-GQ13':[],'TEMP1_I@08QU-QU-855I-GQ14':[], 'TEMP1_I@08QU-QU-855I-GQ15':[],'TEMP1_I@08QU-QU-855I-GQ16':[]}
    
    # INICIO: pos-processamento para garantir que o ROTA1_I@08FI-FL-827I seja igual ao FUNC * max_rot
    for fmin, fmax in constants.production_range:
            f = 'Variables - VarX_{}-{}.csv'.format(fmin, fmax)

            if not os.path.exists(os.path.join(solver_path, f)):                    
                    continue
            df = pd.read_csv(os.path.join(solver_path, f), sep=';', encoding='iso-8859-1')
            
            for line in df.iterrows():
                    var_csv = line[1][' VariableName']
                    var = var_csv
                    coef_real = coef = line[1][' Value']
                    obj_coef_readl = obj_coef = line[1][' ObjCoeff']                                                    
                                        
                    if var in ['TEMP1_I@08QU-QU-855I-GQ01', 'TEMP1_I@08QU-QU-855I-GQA', 'TEMP1_I@08QU-QU-855I-GQB', 'TEMP1_I@08QU-QU-855I-GQC']:
                            continue

                    if 'TEMP1_I@08QU-QU-855I-GQ' not in var:
                            continue
                                    
                    obj_coef_real =    operations.unnormalize_feature(scalers, var, obj_coef, operation='one feature')
                    coef_real =    operations.unnormalize_feature(scalers, var, coef, operation='one feature')                                            
                    
                    if 'TEMP1_I@08QU-QU-855I-GQ' in var:                            
                            rot_func[var].append(coef_real)
                                                     
    return rot_func
    # FIM: pos-processamento para garantir que o ROTA1_I@08FI-FL-827I seja igual ao FUNC * max_rot
    
def define_optimization_results(solver_path, scalers, datasets):
    log.info('define_optimization_results')
    custos_reais = pd.read_csv(f"{solver_path}/costs.csv", decimal=',', sep=';', index_col='TAG').to_dict().get('Custo')
    
    custos_reais = solver_operations.unnormalize_optimization_tags(scalers, custos_reais)
    
    results_otm = []
    inv_tag_2_var = {v: k for k, v in constants.TARGETS_IN_MODEL.items()}
    
    columns = ['faixa', 'TAG', 'minimo', 'maximo', 'valor normalizado', 'valor real', 'custo']
    df_output = pd.DataFrame(columns=columns)
    
    log.debug('DEBUG:')
    dens, gran_ocs, rota_fi = tags_equality(solver_path, scalers, datasets)
    
    rot_func, pot_func = rot_pot_filtragem(solver_path, scalers, datasets)
    temp_gq = increasing_temp_gq(solver_path, scalers, datasets)
    temp_gq = {x:sorted(temp_gq[x]) for x in temp_gq.keys()}    
    
    prod_range = {'750':0, '800':1,'850':2,'900':3,'950':4,'1000':5}
    
    for fmin, fmax in constants.production_range:
            rota_fi[str(fmax)] = np.array(rota_fi[str(fmax)])

            results_otm = []
            f = 'Variables - VarX_{}-{}.csv'.format(fmin, fmax)

            if not os.path.exists(os.path.join(solver_path, f)):
                    log.info(f'Sem solucao para faixa {fmin}, {fmax}')
                    continue
            df_otimizacao = pd.read_csv(os.path.join(solver_path, f), sep=';', encoding='iso-8859-1')
            custo_final = 0
            
            for line in df_otimizacao.iterrows():
                    var_csv = line[1][' VariableName']
                    var = var_csv
                    coef_real = coef = line[1][' Value']
                    obj_coef_readl = obj_coef = line[1][' ObjCoeff']
                    lb = line[1][' LB']
                    ub = line[1][' UB']
                    #if not var.startswith('abrasao') and not var.startswith('COMP') and not var.startswith('rel'): continue

                    if var in ['NIVE1_C@08QU-FR-851I-01M1','NIVE2_I@08QU-FR-851I-01M1','NIVE3_I@08QU-FR-851I-01M1','NIVE4_I@08QU-FR-851I-01M1','NIVE5_I@08QU-FR-851I-01M1','NIVE6_I@08QU-FR-851I-01M1']:
                            svar = 'nive'
                    else:
                            svar = var
                            
                    tag_to_fix = ['SE PP', 'SUP_SE_PP_L@08PR', 'SUP_SE_PR_L@08FI']
                    tag_integer = ['SOMA FUNC FILTROS', 'qtde_discos', 'qtde_moinhos', 'qtde_filtros']

                    if (svar in scalers 
                        and svar not in constants.TARGETS_IN_MODEL.keys() 
                        and svar not in constants.TARGETS_IN_MODEL.values() 
                        and svar not in tag_to_fix 
                        and svar not in tag_integer):

                            if not svar.startswith('FUNC'):
                                coef_real =    operations.unnormalize_feature(scalers, svar, coef, operation='one feature')
                                obj_coef_real =    operations.unnormalize_feature(scalers, svar, obj_coef, operation='one feature')
                                ub =    operations.unnormalize_feature(scalers, svar, ub, operation='one feature')
                                lb =    operations.unnormalize_feature(scalers, svar, lb, operation='one feature')
                                
                            if np.isnan(coef_real):
                                    log.info(f'{svar}, " contains NaN ", {coef_real}')    
                                    coef_real = 0
                 
                    elif svar in scalers and (svar in constants.TARGETS_IN_MODEL.values() or (svar in constants.TARGETS_IN_MODEL) or (svar in tag_to_fix) or (svar in tag_integer)):
                            try:
                                    coef = operations.normalize_feature(scalers, inv_tag_2_var.get(svar), coef_real)
                            except:
                                    if svar in ['Calculo da Energia da Filtragem', 'SUP_SE_PP_L@08PR', 'SUP_SE_PR_L@08FI', 'COMP_MCOMP_PQ_L@08QU']:
                                        ub =    operations.unnormalize_feature(scalers, svar, ub, operation='one feature')
                                        lb =    operations.unnormalize_feature(scalers, svar, lb, operation='one feature')
                                        coef = coef_real
                                        coef_real = operations.unnormalize_feature(scalers, svar, coef_real, operation='one feature')
                                    else:
                                        coef = operations.normalize_feature(scalers, svar, coef_real)
                            
                    custo_final += coef_real * custos_reais.get(var, 0)
                    
                    
                    if 'DENS1_C@08HO-BP-826I-' in svar:                            
                            coef_real = dens
                    
                    if 'GRAN_OCS_TM@08PE-BD-840I' in svar:
                            coef_real = gran_ocs[int(svar[-2:])-1]
                            log.debug(f'DEBUG:  {svar}, {coef_real}')

                    if 'ROTA1_I@08FI-FL-827I-' in svar:
                            rot = rota_fi[str(fmax)].nonzero()
                            coef_real = rota_fi[str(fmax)][rot].mean() * rot_func[svar][str(fmax)]
                    
                    if 'POTE1_I@08FI-BV-827I-' in svar:                            
                            coef_real = coef_real * pot_func[svar][str(fmax)]                            
                            
                    if 'TEMP1_I@08QU-QU-855I-GQ' in svar and svar not in ['TEMP1_I@08QU-QU-855I-GQ01', 'TEMP1_I@08QU-QU-855I-GQA', 'TEMP1_I@08QU-QU-855I-GQB', 'TEMP1_I@08QU-QU-855I-GQC']:                            
                            coef_real = temp_gq[svar][prod_range[str(fmax)]]
                            
                    if var in inv_tag_2_var:
                            if inv_tag_2_var[var] in [datasets[qualidade].columns[-1] for qualidade in ['compressao']]:   
                                    if var != 'SE PP':
                                        coef_real = np.exp(coef_real)
                                        ub = np.exp(ub)
                                        lb = np.exp(lb)

                    results_otm.append(['{}-{}'.format(fmin, fmax), var, lb, ub, coef, coef_real, (custos_reais.get(var, 0))])
            
            df = pd.DataFrame(results_otm, columns=columns)
            
            df_output = pd.concat([df_output, df])
            log.info('Faixa de produção: {:4d}-{:4d} - Custo: {:.3f}'.format(fmin, fmax, custo_final))
    
    cfix_mean = df_output.loc[df_output['TAG'] == 'cfix', 'valor real'].mean()
    df_output.loc[df_output['TAG'] == 'cfix', 'valor real'] = cfix_mean
    df_output.loc[df_output['TAG'] == 'cfix', 'valor normalizado'] = scalers['QUIM_CFIX_PP_L@08PR'].transform([[cfix_mean]])[0][0]
    
    solver_operations.save_solver_results(solver_path, df_output)

    return df_output