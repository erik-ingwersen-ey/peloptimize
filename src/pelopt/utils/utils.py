import json
import os
import uuid
import datetime

import joblib
import pandas as pd
import pyodbc
import pytz

import pulp


# Recupera idPred da tabela FPred
def getIDPredSeq():
    driver = "{ODBC Driver 17 for SQL Server}"
    sqlserver = "sqldb-vale-lab.database.windows.net"
    port = "1433"
    database = "sqldb-vale-lab-01"
    user = "CeLoader"
    pswd = dbutils.secrets.get(
        key="sqldb-vale-lab-01", scope="[scope_password_protection]"
    )

    cnxn = pyodbc.connect(
        "DRIVER="
        + driver
        + ";SERVER="
        + sqlserver
        + ";PORT=1433;DATABASE="
        + database
        + ";UID="
        + user
        + ";PWD="
        + pswd
    )
    cursor = cnxn.cursor()

    cursor.execute("SELECT MAX(idPred) FROM PEL_OP.FPRED")
    row = cursor.fetchone()

    cursor.commit()
    cnxn.close()
    if len(row) > 0:
        return row[0] + 1
    else:
        return 1


# Recupera idFVar da tabela FVar
def getIDVarSeq():
    driver = "{ODBC Driver 17 for SQL Server}"
    sqlserver = "sqldb-vale-lab.database.windows.net"
    port = "1433"
    database = "sqldb-vale-lab-01"
    user = "CeLoader"
    pswd = dbutils.secrets.get(
        key="sqldb-vale-lab-01", scope="[scope_password_protection]"
    )

    cnxn = pyodbc.connect(
        "DRIVER="
        + driver
        + ";SERVER="
        + sqlserver
        + ";PORT=1433;DATABASE="
        + database
        + ";UID="
        + user
        + ";PWD="
        + pswd
    )
    cursor = cnxn.cursor()

    cursor.execute("SELECT MAX(idFVar) FROM PEL_OP.FVAR")
    row = cursor.fetchone()

    cursor.commit()
    cnxn.close()
    if len(row) > 0:
        return row[0] + 1
    else:
        return 1


# Recupera idUsina_modelo da tabela DUsina_modelo
def getIDUsinaModelo(modelo, usina):
    driver = "{ODBC Driver 17 for SQL Server}"
    sqlserver = "sqldb-vale-lab.database.windows.net"
    port = "1433"
    database = "sqldb-vale-lab-01"
    user = "CeLoader"
    pswd = dbutils.secrets.get(
        key="sqldb-vale-lab-01", scope="[scope_password_protection]"
    )

    cnxn = pyodbc.connect(
        "DRIVER="
        + driver
        + ";SERVER="
        + sqlserver
        + ";PORT=1433;DATABASE="
        + database
        + ";UID="
        + user
        + ";PWD="
        + pswd
    )
    cursor = cnxn.cursor()

    cursor.execute(
        "select idUsina_modelo from pel_op.DUsina_modelo where idModelo = {} "
        "and idUsina = {}".format(modelo, usina)
    )
    row = cursor.fetchone()

    cursor.commit()
    cnxn.close()

    return row[0]


##############################################################################################################################################
# funções para carregar e salvar no datalake
##############################################################################################################################################
# # Carrega modelos do datalake
# def load_adls(path, filename):
#   try:
#     copy_path = 'dbfs:/tmp/'
#     dbutils.fs.cp(os.path.join(path, filename), os.path.join(copy_path,
#     filename))
#     load_path = '/dbfs/tmp/'
#     obj = joblib.load(os.path.join(load_path, filename))
#   except Exception as e:
#     print(e)
#     return False
#   finally:
#     dbutils.fs.rm(os.path.join(copy_path, filename), recurse=True)
#   return obj

# Carrega modelos do datalake
def load_adls(path, filename, dbutils):
    copy_path = "dbfs:/tmp/"
    dbutils.fs.cp(
        os.path.join(path, filename), os.path.join(copy_path, filename)
    )
    load_path = "/dbfs/tmp/"
    obj = joblib.load(os.path.join(load_path, filename))

    dbutils.fs.rm(os.path.join(copy_path, filename), recurse=True)

    return obj


# save_LP_adls -- Grava o LP no datalake
def save_LP_adls(path, filename, solver, dbutils):
    dump_path = "/dbfs/tmp/"
    copy_path = "dbfs:/tmp/"

    try:
        #     joblib.dump(obj, os.path.join(dump_path, filename))
        solver.probs[filename].writeLP(
            os.path.join(dump_path, filename + ".lp")
        )
        dbutils.fs.cp(
            os.path.join(copy_path, filename + ".lp"),
            os.path.join(path, filename + ".lp"),
        )
    except Exception as e:
        print(e)
        return False
    finally:
        dbutils.fs.rm(os.path.join(copy_path, filename + ".lp"), recurse=True)


# Salva modelos do datalake
def save_model_adls(path, filename, obj, dbutils):
    dump_path = "/dbfs/tmp/"
    copy_path = "dbfs:/tmp/"

    try:
        joblib.dump(obj, os.path.join(dump_path, filename))
        dbutils.fs.cp(
            os.path.join(copy_path, filename), os.path.join(path, filename)
        )
    except Exception as e:
        print(e)
        return False
    finally:
        dbutils.fs.rm(os.path.join(copy_path, filename), recurse=True)


# load_csv -- Carrega CSVs do datalake
def load_csv(path, filename):
    try:
        copy_path = "dbfs:/tmp/"
        dbutils.fs.cp(
            os.path.join(path, filename), os.path.join(copy_path, filename)
        )
        load_path = "/dbfs/tmp/"
        obj = pd.read_csv(
            os.path.join(load_path, filename),
            sep=";",
            index_col=0,
            dtype="unicode",
        )
    except Exception as e:
        print(e)
        return False
    finally:
        dbutils.fs.rm(os.path.join(copy_path, filename), recurse=True)
    return obj


# save_csv -- Grava CSVs do datalake
def save_csv(path, filename, obj, dbutils, sep=","):
    dump_path = "/dbfs/tmp/"
    copy_path = "dbfs:/tmp/"

    try:
        obj.to_csv(
            os.path.join(dump_path, filename),
            sep=sep,
            encoding="ISO-8859-1",
            index=False,
        )
        dbutils.fs.cp(
            os.path.join(copy_path, filename), os.path.join(path, filename)
        )
    except Exception as e:
        print(e)
        return False
    finally:
        dbutils.fs.rm(os.path.join(copy_path, filename), recurse=True)


# # Grava XLSX no local e depois copia para o Datalake
# def save_model_adls_xls(path, filename):
#     import shutil

#     dump_path = '/dbfs/temp/'
#     copy_path = 'dbfs:/temp/'

#     shutil.copy('info_modelos.xlsx', '/dbfs/temp/info_modelos.xlsx')

#     try:
#         dbutils.fs.cp(os.path.join(copy_path, filename), os.path.join(path,
#         filename))
#     except Exception as e:
#         print(e)
#         return False
#     finally:
#         dbutils.fs.rm(os.path.join(copy_path, filename), recurse=True)

# Salva modelos do datalake - Marcelo
def save_model_adls_xls(path, filename, obj):
    dump_path = "/dbfs/tmp/"
    copy_path = "dbfs:/tmp/"

    try:
        joblib.dump(obj, os.path.join(dump_path, filename))
        dbutils.fs.cp(
            os.path.join(copy_path, filename), os.path.join(path, filename)
        )
    except Exception as e:
        print(e)
        return False
    finally:
        dbutils.fs.rm(os.path.join(copy_path, filename), recurse=True)


# Carrega JSON do datalake
def load_adls_json(path, filename):
    try:
        copy_path = "dbfs:/tmp/"
        dbutils.fs.cp(
            os.path.join(path, filename), os.path.join(copy_path, filename)
        )
        load_path = "/dbfs/tmp/"
        with open(os.path.join(load_path, filename)) as f:
            obj = json.load(f)
    except Exception as e:
        print(e)
        return False
    finally:
        dbutils.fs.rm(os.path.join(copy_path, filename), recurse=True)
    return obj


##############################################################################################################################################
# section: funções novas - Marcelo
##############################################################################################################################################
# save_txt -- Grava CSVs do datalake
def save_txt(path, filename, obj, dbutils):
    dump_path = "/dbfs/tmp/"
    copy_path = "dbfs:/tmp/"

    try:
        with open(os.path.join(dump_path, filename), "w") as f:
            f.write(obj)
        dbutils.fs.cp(
            os.path.join(copy_path, filename), os.path.join(path, filename)
        )
    except Exception as e:
        print(e)
        return False
    finally:
        dbutils.fs.rm(os.path.join(copy_path, filename), recurse=True)


# save_json -- Grava JSONs do datalake - nao funciona
def save_json(path, filename, obj):
    dump_path = "/dbfs/tmp/"
    copy_path = "dbfs:/tmp/"

    try:
        json.dump(json.dumps(obj), os.path.join(dump_path, filename))
        dbutils.fs.cp(
            os.path.join(copy_path, filename), os.path.join(path, filename)
        )
    except Exception as e:
        print(e)
        return False
    finally:
        dbutils.fs.rm(os.path.join(copy_path, filename), recurse=True)


# save_csv -- Grava CSVs do datalake - Marcelo - nao funciona
def save_excel(path, filename, obj):
    dump_path = "/dbfs/tmp/"
    copy_path = "dbfs:/tmp/"

    try:
        obj = obj.toPandas()
        obj.to_excel(
            os.path.join(dump_path, filename), index=False, engine="openpyxl"
        )  # encoding="ISO-8859-1",
        dbutils.fs.cp(
            os.path.join(copy_path, filename), os.path.join(path, filename)
        )
    except Exception as e:
        print(e)
        return False
    finally:
        dbutils.fs.rm(os.path.join(copy_path, filename), recurse=True)


##############################################################################################################################################
# continua
##############################################################################################################################################
# Recupera idUsina_modelo da tabela DUsina_modelo
def getFVarMaxDate(idTAG, usina_modelo, operator):
    driver = "{ODBC Driver 17 for SQL Server}"
    sqlserver = "sqldb-vale-lab.database.windows.net"
    port = "1433"
    database = "sqldb-vale-lab-01"
    user = "CeLoader"
    pswd = dbutils.secrets.get(
        key="sqldb-vale-lab-01", scope="[scope_password_protection]"
    )

    cnxn = pyodbc.connect(
        "DRIVER="
        + driver
        + ";SERVER="
        + sqlserver
        + ";PORT=1433;DATABASE="
        + database
        + ";UID="
        + user
        + ";PWD="
        + pswd
    )
    cursor = cnxn.cursor()

    if operator == "==":
        cursor.execute(
            "select MAX(data) from pel_op.FVar where idVariavel = {} and "
            "idUsina_modelo = {}".format(idTAG, usina_modelo)
        )
    else:
        cursor.execute(
            "select MAX(data) from pel_op.FVar where idVariavel <> {} and "
            "idUsina_modelo = {}".format(idTAG, usina_modelo)
        )
    row = cursor.fetchone()

    cursor.commit()
    cnxn.close()

    return row


# Recupera idUsina_modelo da tabela DUsina_modelo
def getSQLDates(usina, modelo):
    import time

    driver = "{ODBC Driver 17 for SQL Server}"
    sqlserver = "sqldb-vale-lab.database.windows.net"
    port = "1433"
    database = "sqldb-vale-lab-01"
    user = "CeLoader"
    pswd = dbutils.secrets.get(
        key="sqldb-vale-lab-01", scope="[scope_password_protection]"
    )

    count = 0
    while count < 4:
        try:
            cnxn = pyodbc.connect(
                "DRIVER="
                + driver
                + ";SERVER="
                + sqlserver
                + ";PORT=1433;DATABASE="
                + database
                + ";UID="
                + user
                + ";PWD="
                + pswd
            )
            cursor = cnxn.cursor()

            cursor.execute(
                "select MAX(data_predicao) from pel_op.FPred where idUsina = "
                "{} and idModelo = {}".format(usina, modelo)
            )
            row = cursor.fetchone()

            cursor.commit()
            cnxn.close()

            tz = pytz.timezone("America/Sao_Paulo")
            hora_agora = datetime.datetime.now(tz)
            hora_agora = hora_agora.replace(microsecond=0, second=0, minute=0)

            if None in row:
                ultima_previsao = datetime.datetime(2019, 1, 1, 0, 0, 0)
            else:
                ultima_previsao = pd.to_datetime(row[0])

            tini = ultima_previsao - datetime.timedelta(hours=72)
            tfim = hora_agora
            break
        except:
            time.sleep(15)
            count = count + 1

    if count < 4:
        return tini, tfim, ultima_previsao
    else:
        return False, False, False


# Busca a data da ultima predicao e retorna as datas inciais, finais e
# ultima_previsao
def getSQLDates2(dicParametros):
    tz = pytz.timezone("America/Sao_Paulo")
    hora_agora = datetime.datetime.now(tz)
    hora_agora = hora_agora.replace(microsecond=0, second=0, minute=0)

    jdbcDF = (
        spark.read.format("jdbc")
        .option(
            "url",
            "jdbc:sqlserver://sqldb-vale-lab.database.windows.net:1433"
            ";databaseName=sqldb-vale-lab-01",
        )
        .option("dbtable", "pel_op.FPred")
        .option("user", "CeLoader")
        .option(
            "password",
            dbutils.secrets.get(
                key="sqldb-vale-lab-01", scope="[scope_password_protection]"
            ),
        )
        .load()
    )

    jdbcDF = jdbcDF.select("*").toPandas()
    #   jdbcDF = jdbcDF[jdbcDF['idModelo'] == dicParametros['modelo']]
    jdbcDF = jdbcDF[
        (jdbcDF["idModelo"] == dicParametros["modelo"])
        & (jdbcDF["idUsina"] == dicParametros["usina"])
    ]
    if jdbcDF.shape[0] == 0:
        ultima_previsao = datetime.datetime(2019, 1, 1, 0, 0, 0)
    else:
        ultima_previsao = (
            jdbcDF.groupby("idModelo")["data_predicao"].max().values[0]
        )
        ultima_previsao = pd.to_datetime(ultima_previsao)

    tini = ultima_previsao - datetime.timedelta(hours=72)
    tfim = hora_agora

    return tini, tfim, ultima_previsao


# retorna string contendo "disco" + numero do disco em formato 0
def disco(num):
    """
    retorna string contendo "disco" + numero do disco em formato 0#
    ex: 'disco01'
    """
    return "disco" + str(num).zfill(2)


# funcoes de formatacao da variavel
def format_key(k):
    if "_" in k:
        return "".join(s[0].upper() for s in k.split("_"))
    return k[:4]


def format_value(v):
    if v is True:
        return "Y"
    if v is False:
        return "N"
    if v is None:
        return "-"
    return v


def params_to_str(params):
    return ";".join(
        "{}={}".format(format_key(k), format_value(v))
        for k, v in params.items()
        if k not in {"random_state", "seed"}
    )


# Ler DF fato VAR do banco e buscar ultimo seq
def getSeq(nomeTabela, nomeColuna):
    df_FVar = (
        spark.read.format("jdbc")
        .option(
            "url",
            "jdbc:sqlserver://sqldb-vale-lab.database.windows.net:1433"
            ";databaseName=sqldb-vale-lab-01",
        )
        .option("dbtable", nomeTabela)
        .option("user", "CeLoader")
        .option(
            "password",
            dbutils.secrets.get(
                key="sqldb-vale-lab-01", scope="[scope_password_protection]"
            ),
        )
        .load()
    )

    if df_FVar.count() == 0:
        seqVar = 1
    else:
        seqVar = df_FVar.agg({nomeColuna: "max"}).collect()[0][0] + 1

    return seqVar


# formatacao de dataframe que vai escrever no banco SQL
def completarTabela(dfVAR, dicTarget, dicParametros):
    dfVAR["name"] = dfVAR["name"].map(dicTags)  # coloca nome original da tag
    dfVAR["target"] = 1
    dfVAR["idUsina"] = dicParametros["usina"]
    dfVAR["idModelo"] = dicParametros["modelo"]
    dfVAR["idUsina_modelo"] = dicParametros["usina_modelo"]
    #   dfVAR['idVariavel'] = [buscaIDVariavel(x, df_DVariavel,
    #   dicParametros) for x in dfVAR['name']]
    dfVAR["idVariavel"] = [dicTarget for x in dfVAR["name"]]

    dfVAR["data"] = pd.to_datetime(dfVAR["ts"], unit="ms")
    dfVAR.rename(columns={"value": "valor"}, inplace=True)

    seqVar = getSeq("pel_op.FVar", "idFVar")  # preenche com id sequencial
    dfVAR["idFVar"] = list(range(seqVar, seqVar + dfVAR.shape[0]))

    cols = [
        "idFVar",
        "idModelo",
        "idUsina",
        "idUsina_modelo",
        "idVariavel",
        "data",
        "valor",
        "target",
    ]
    dfVAR = dfVAR[cols]
    dfVAR.sort_values(by="data", ascending=True, inplace=True)
    return dfVAR


# inicializa dataframe e formata datas de inicio e fim
def trataTabela(modelo, tabDatas, dicTarget, dicParametros):
    dfVAR = pd.DataFrame()
    tini = tabDatas.loc[modelo, "ult_dt_target"] + datetime.timedelta(hours=1)
    tfim = tabDatas.loc[modelo, "ult_dt_var"]
    sql2 = geraSQL2(
        tini, tfim, tabDatas.loc[modelo, "targetdl"]
    )  # puxa dados e converte df spark para df pandas
    dfVAR = sql(sql2).select("*")
    dfVAR = dfVAR.toPandas()
    dfVAR = completarTabela(dfVAR, dicTarget, dicParametros)

    print(modelo, dfVAR["data"].min(), dfVAR["data"].max())
    return dfVAR


# Salvar target no banco
def salvaTarget(dfVAR):
    jdbcUrl = (
        "jdbc:sqlserver://sqldb-vale-lab.database.windows.net:1433"
        ";databaseName=sqldb-vale-lab-01"
    )
    sqlContext.createDataFrame(dfVAR).write.format("jdbc").option(
        "url", jdbcUrl
    ).option("dbtable", "pel_op.FVar").option("user", "CeLoader").option(
        "password",
        dbutils.secrets.get(
            key="sqldb-vale-lab-01", scope="[scope_password_protection]"
        ),
    ).mode(
        "append"
    ).save()


# verifica se dataframe nao esta vazio
def checkdf(dfVAR):
    cont = False
    if dfVAR.shape[0] > 0:
        cont = True
    return cont


# busca ID da variaval no banco SQL
def buscaIDVariavel(x, df_DVariavel, dicParametros):
    tmp = df_DVariavel.copy()
    tmp = tmp[tmp["idUsina_modelo"] == dicParametros["usina_modelo"]]
    idVariavel = tmp.loc[tmp["descricao"] == x, "idVariavel"]
    if len(idVariavel) > 0:
        return idVariavel.values[0]
    else:
        return x


# busca ID da variaval no banco SQL
def buscaIDVariavel2(x, df_DVariavel):
    idVariavel = df_DVariavel.loc[df_DVariavel["descricao"] == x, "idVariavel"]
    if len(idVariavel) > 0:
        return idVariavel.values[0]
    else:
        return x


# Recupera idUsina_modelo da tabela DUsina_modelo
def getIDVariavel(usina_modelo):
    driver = "{ODBC Driver 17 for SQL Server}"
    sqlserver = "sqldb-vale-lab.database.windows.net"
    port = "1433"
    database = "sqldb-vale-lab-01"
    user = "CeLoader"
    pswd = dbutils.secrets.get(
        key="sqldb-vale-lab-01", scope="[scope_password_protection]"
    )

    cnxn = pyodbc.connect(
        "DRIVER="
        + driver
        + ";SERVER="
        + sqlserver
        + ";PORT=1433;DATABASE="
        + database
        + ";UID="
        + user
        + ";PWD="
        + pswd
    )
    cursor = cnxn.cursor()

    cursor.execute(
        "select idVariavel, descricao from pel_op.DVariavel where "
        "idUsina_modelo = {}".format(usina_modelo)
    )
    row = cursor.fetchall()

    cursor.commit()
    cnxn.close()

    return row


##############################################################################################################################################
# section: funções para gerar excel e csv
##############################################################################################################################################
# gera excel info_modelos no cluster do databricks
def info_modelos(results, models):
    """
    Creates an .xlsx file with the models information
    Args:
      results (dict): dictionary with models dump information
      models (list): list with models that will be in the excel file (to use
      all models, models = results.keys())
    """
    workbook = xlsxwriter.Workbook("info_modelos.xlsx")
    worksheet_mape = workbook.add_worksheet("Info Modelos")
    worksheet_mape.write(0, 0, "Modelo")
    worksheet_mape.write(0, 1, "Mape")
    worksheet_mape.write(0, 2, "Intercepto")
    row_mape = 1
    coeficientes_modelos = {}

    for qualidade in models:
        worksheet = workbook.add_worksheet(qualidade)

        coeficientes_modelos[qualidade] = {}

        best = sorted(results[qualidade], key=lambda e: e["metrics"]["mape"])[0]

        worksheet_mape.write(row_mape, 0, qualidade)
        worksheet_mape.write(
            row_mape, 1, "{:6.2f}%".format(best["metrics"]["mape"])
        )
        worksheet_mape.write(
            row_mape, 2, "{:6.2f}".format(best["model"].intercept_)
        )
        row_mape += 1
        worksheet.write(0, 0, "Tag")
        worksheet.write(0, 1, "Coeficiente")
        worksheet.write(0, 2, "Descricao")
        row = 1

        a = sorted(
            zip(best["columns"], best["model"].coef_), key=lambda e: -abs(e[1])
        )
        for e in a:
            coeficientes_modelos[qualidade][e[0]] = e[1]

            worksheet.write(row, 0, e[0])
            worksheet.write(row, 1, "{:.5f}".format(e[1]))
            row += 1
    workbook.close()


# adiciona novas tags para o modelo
def add_model_tags(datasets, df, qual, tags):
    """
    Add new tags to specified model
    Args:
      datasets (Dataframe): final dataframe
      df (Dataframe): SQL dataframe
      qual (str): specified model
      tags (list): tags to include
    """

    try:
        __ = df[tags]
    except:
        raise Exception(
            str([x for x in tags if x not in df.columns])
            + " Não encontradas no dataset"
        )

    filtered_tags = tags
    if qual in datasets:
        filtered_tags = list(set(df[tags]).difference(datasets[qual].columns))
    if len(filtered_tags) == 0:
        return

    if qual in datasets.keys():
        datasets[qual] = datasets[qual].join(df[filtered_tags])
    else:
        datasets[qual] = df[filtered_tags]
        datasets[qual].index.name = "Processo"

    cols = datasets[qual].columns.tolist()
    datasets[qual] = datasets[qual][filtered_tags + cols[: -len(filtered_tags)]]
