from asyncio.log import logger
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import re
import pyodbc
import time
import pytz

from pelopt.config import TZ_SAO_PAULO
from pelopt.utils.model_module import format_key, format_value
from pelopt.utils.utils import completarTabela, getSeq


class SqlCredentials:
    driver = "{ODBC Driver 17 for SQL Server}"
    sqlserver = "usazu1server0447.database.windows.net"
    port = "1433"
    database = "usazu1-q-db2-0447"
    user = "app_pelletizing"
    prefix = "pel_op"
    _pswd = None

    @property
    def pswd(self) -> str:
        if self._pswd is None:
            if hasattr(self, 'dbutils'):
                self._pswd = self.dbutils.secrets.get(
                    key=self.database, scope="[scope_password_protection]"
                )
            else:
                raise AttributeError(
                    "`dbutils` not found. Please set the dbutils attribute"
                    " or the pswd attribute."
                )
        return self._pswd

    @pswd.setter
    def pswd(self, value: str):
        self._pswd = value

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)


class sqlModule(SqlCredentials):
    _cnxn = None

    @property
    def cnxn(self):
        if self._cnxn is None:
            (
                driver, sqlserver, port, database, user, pswd, prefix,
            ) = self.dbInfo()
            self._cnxn = pyodbc.connect(
                f"DRIVER={driver};SERVER={sqlserver};PORT={1433};DATABASE="
                f"{database};UID={user};PWD={pswd}"
            )
        return self._cnxn

    @property
    def cursor(self):
        return self.cnxn.cursor()

    def __init__(self, dbutils=None, spark=None):
        """Os parâmetros dbutils e spark são necessários para herdar funções
        pré-definidas no escopo de notebooks do databricks.
        Caso o projeto seja migrado futuramente para Repos (git),
        tais funções apenas funcionam em script python via herança.
        """
        self.dbutils = dbutils
        self.spark = spark
        super().__init__()

    def dbInfo(self, option=1):
        """Define variáveis de acesso ao banco de dados.
        Sobre senha definida via dbutils.secrets, acessar
        HowTo/UsingPasswords no Workspace
        Option 2 e 3 não são usadas. Foram criadas pela Accenture. Mantive
        como legado.
        """
        driver = "{ODBC Driver 17 for SQL Server}"
        sqlserver = "usazu1server0447.database.windows.net"
        port = "1433"
        database = "usazu1-q-db2-0447"
        user = "app_pelletizing"
        pswd = self.dbutils.secrets.get(
            key="usazu1-q-db2-0447", scope="[scope_password_protection]"
        )
        prefix = "pel_op"
        return driver, sqlserver, port, database, user, pswd, prefix

    def getDateLabs(self, taglab, usina, param=""):
        """Recupera TODAS as DATAS de inserção das TAGs de LAB da tabela
        ``UltDataTag``.

        Parameters
        ----------
        taglab : list
            lista de TAGs a serem buscadas na coluna variável do ``UltDataTag``
            Input data.
        usina : int
        param : str, default ''

        Local variables, sql tables
        ----------
        UltDataTag : sql table
            armazena a última data de inserção das tags de laboratório
        sqlVar : str
            variável usada na execução de query para restringir (via WHERE)
            as tags selecionadas
        """
        sqlVar = ""

        # exemplo do string: variável = var1 or variável = var2 ...
        for var in taglab:
            sqlVar += "variavel = ? or "

        cursor = self.cursor

        cursor.execute(
            f"select variavel, data, idusina from {prefix}.UltDataTag where "
            f"idusina = ? and {sqlVar[:-4]} ",
            [usina] + taglab,
        )
        row = cursor.fetchall()
        cursor.commit()
        self.cnxn.close()

        # Gera dados desde 2019 para todas as TAGs.
        if len(row) == 0:
            tagData = self.getLabAllDate(taglab, usina, param)

            ult_dado = tagData.copy().rename(columns={"max_date": "data"})
            # No teste do PIsystem, var usina OK, vai buscar usina 108
            ult_dado["idusina"] = usina

            writeControl = 1
            while writeControl < 5:
                try:
                    (
                        sqlContext.createDataFrame(ult_dado)
                        .write.format("jdbc")
                        .option(
                            "url",
                            f"jdbc:sqlserver://"
                            f"{self.sqlserver}:1433;databaseName="
                            f"{self.database}",
                        )
                        .option("dbtable", f"{self.prefix}.UltDataTag")
                        .option("user", self.user)
                        .option("password", self.pswd)
                        .mode("append")
                        .save()
                    )

                    break
                except Exception as err:
                    time.sleep(20 * writeControl)
                    writeControl += 1
                    logger.exception(err)
            ult_dado = tagData.copy()

        # Gera dados desde 2019, quando não encontra a TAG.
        # Pega ultima data para o restante.
        elif len(row) != len(taglab):
            ult_dado = pd.DataFrame.from_records(
                row, columns=[column[0] for column in cursor.description]
            )
            tagData = self.getLabAllDate(
                (set(taglab) - set([i[0] for i in row])), usina, param
            )
            ult_dado = pd.concat([ult_dado, tagData])
            ult_dado = ult_dado.reset_index(drop=True).drop(
                columns=["max_date"], errors="ignore"
            )
        # Pega última data para todas as TAGs.
        else:
            ult_dado = pd.DataFrame.from_records(
                row, columns=[column[0] for column in cursor.description]
            )
        return ult_dado

    # getLabAllDate Recupera TODAS as DATAS de inserção das TAGs de LAB
    # usado em getDateLabs mais acima
    # no teste do PIsystem, var usina está errado, vai buscar usina 108 numa
    # tabela que só tem usina 8, na verdade, não, pois vai chamar a tabela nova.
    def getLabAllDate(self, taglab, usina, param=""):
        sqlVar = ""
        for var in taglab:
            sqlVar = sqlVar + "variavel = ? or "

        cnxn = self.cnxn
        cursor = self.cursor

        # captura a data maxima de cada variável do ``taglab``
        cursor.execute(
            f"select variavel, MAX(data) as max_date from {prefix}.fVarpims_"
            f"{param} where idUsina = ? and calculado = 0 and {sqlVar[:-4]} "
            f"GROUP BY variavel",
            [usina] + list(taglab),
        )
        row = cursor.fetchall()
        cursor.commit()
        cnxn.close()

        # Replicando o if elif else muito similar à função pai
        # Gera dados desde 2019 para todas as tags.
        if len(row) == 0:
            ult_dado = pd.DataFrame(taglab, columns=["variavel"])
            ult_dado["max_date"] = datetime(2019, 1, 1, 0, 0, 0)
        # Gera dados desde 2019 quando nao encontra a tag. Pega ultima data
        # Para o restante.
        elif len(row) != len(taglab):
            # DEBUG: renomear parâmetro taglab para taglab de modo
            # que pegue a variável local em vez da variável global SEMPRE.
            # Erro grave.
            ult_dado = pd.DataFrame.from_records(
                row, columns=[column[0] for column in cursor.description]
            )
            for tagMissing in set(taglab) - set([i[0] for i in row]):
                df_tmp = pd.DataFrame(
                    [[tagMissing, datetime(2019, 1, 1, 0, 0, 0)]],
                    columns=["variavel", "max_date"],
                )
                ult_dado = pd.concat([ult_dado, df_tmp])
                ult_dado = ult_dado.reset_index(drop=True)
        # Pega ultima data para todas as tags.
        else:
            ult_dado = pd.DataFrame.from_records(
                row, columns=[column[0] for column in cursor.description]
            )

        return ult_dado

    # getLabMaxDate Recupera a última DATA de inserção das tags de LAB
    def getLabMaxDate(self, taglab, usina):

        cnxn = self.cnxn
        cursor = self.cursor
        cursor.execute(
            "select MAX(data) from pel_op.fVarpims where variavel = '{}' and "
            "idUsina = {} and calculado = 0".format(taglab, usina)
        )
        row = cursor.fetchone()
        cursor.commit()
        cnxn.close()
        ult_dado = pd.to_datetime(row[0])
        if None in row:
            ult_dado = datetime(2019, 1, 1, 0, 0, 0)
        return ult_dado

    # REF getMinPredDate — Recupera da que modelo foi predito mais ANTIGA,
    # para poder recuperar os dados necessários para predizer todos os modelos
    def getMinPredDate(self, usina):

        count = 0
        while count < 4:
            try:
                cnxn = self.cnxn
                cursor = self.cursor
                cursor.execute(
                    "select MIN(ult_pred), count(ID) from "
                    + prefix
                    + ".UltData where idUsina = {} and idModelo <> 0".format(
                        usina
                    )
                )
                row = cursor.fetchone()

                cursor.commit()
                cnxn.close()

                tz = pytz.timezone("America/Sao_Paulo")
                hora_agora = datetime.now(tz)
                hora_agora = hora_agora.replace(
                    microsecond=0, second=0, minute=0
                )

                if None in row:
                    ultima_previsao = datetime(2019, 1, 1, 0, 0, 0)
                elif (row[1] < 5) & (
                    usina < 9
                ):  # verifica se todos os modelos possuem dados
                    ultima_previsao = datetime(2019, 1, 1, 0, 0, 0)
                else:
                    ultima_previsao = pd.to_datetime(row[0])

                tini = ultima_previsao - datetime.timedelta(hours=12)
                tfim = hora_agora
                break
            except Exception as err:  # pylint: disable=broad-except
                logger.exception(err)
                time.sleep(15)
                count += 1
        if count < 4:
            return tini, tfim, ultima_previsao
        else:
            return False, False, False

    # REF getSQLDates -- Recupera ult_pred da tabela pel_op2.UltData
    def getSQLDates(self, usina, modelo):

        count = 0
        while count < 4:
            try:
                cnxn = self.cnxn
                cursor = self.cursor

                cursor.execute(
                    "select ult_pred from " + prefix + ".UltData where "
                    "idUsina = {} and "
                    "idModelo = {}".format(usina, modelo)
                )
                row = cursor.fetchone()

                cursor.commit()
                cnxn.close()

                tz = pytz.timezone("America/Sao_Paulo")
                hora_agora = datetime.now(tz)
                hora_agora = hora_agora.replace(
                    microsecond=0, second=0, minute=0
                )
                ultima_previsao = pd.to_datetime(row[0])
                if row is None:
                    ultima_previsao = datetime(2019, 1, 1, 0, 0, 0)
                tini = ultima_previsao - datetime.timedelta(hours=72)
                tfim = hora_agora
                break
            except Exception as err:  # pylint: disable=broad-except
                logger.exception(err)
                time.sleep(15)
                count += 1
        if count < 4:
            return tini, tfim, ultima_previsao
        return False, False, False

    # nao é usado
    # getIDUsinaModelo -- Recupera idUsina_modelo da tabela DUsina_modelo
    def getIDUsinaModelo(self, modelo, usina):
        cnxn = self.cnxn
        cursor = self.cursor
        cursor.execute(
            "select idUsina_modelo from " + prefix + ".DUsina_modelo where "
            "idModelo = {} and "
            "idUsina = {}".format(modelo, usina)
        )
        row = cursor.fetchone()
        cursor.commit()
        cnxn.close()
        return row[0]

    # getLabData Recupera os DADOS de LAB antigos
    def getLabData(self, tagpims, tini, tfim):
        """Retrieve all data from old LAB Tags.

        Parameters
        ----------
        tagpims : List[str]
            List of tags that are used to diagnose the pelletizing process.
        tini : datetime
            Initial date to retrieve data.
        tfim : datetime
            Final date to retrieve data.

        Returns
        -------
        pd.DataFrame
            DataFrame with all data from old LAB Tags.
        """
        # Gera df_lab dependendo do grupo de usinas: Tubarão, SL, VGR
        tagdl = self.tagPIMStoDataLake(tagpims)
        taglist = pd.DataFrame({"tagdl": tagdl, "tagpims": tagpims})
        if "@1N" in taglist["tagpims"][0]:
            sql1 = sqlModule().geraSQL1(
                tini, tfim, taglist, source="PIMSDIPESAOLUIS"
            )
        elif taglist["tagpims"][0].lower().startswith("vgr"):
            sql1 = sqlModule().geraSQL1(
                tini, tfim, taglist, source="PIMSDIPEVGR"
            )
        else:
            sql1 = sqlModule().geraSQL1(
                tini, tfim, taglist, source="PIMSDIPETUBARAO"
            )
        df_lab = self.spark.sql(sql1).select("*")
        if df_lab.count() > 0:
            writeControl = 0
            while writeControl < 5:
                try:
                    df_lab = df_lab.toPandas()
                    break
                except Exception as err:
                    logger.exception(err)
                    time.sleep(10 * writeControl)
                    writeControl += 1

            df_lab.ts = pd.to_datetime(df_lab.ts, unit="ms")
            dicTags = dict(zip(taglist.tagdl, taglist.tagpims))
            df_lab.name = df_lab.name.map(dicTags)
            return df_lab
        return None

    # getLabData Recupera os DADOS de LAB antigos no PI System
    def getLabData_pisystem(self, tagpims, taglist, tini, tfim, n_usina):
        taglist = taglist[taglist["tagpims"].isin(tagpims)]
        # gera `df_lab` dependendo do grupo de usinas: Tubarão, SL, VGR
        tagdl = taglist.tagdl
        # geraSQL_pisystem = []
        sql1 = sqlModule().geraSQL_pisystem(
            tini,
            tfim,
            taglist,
            source="PIMSDIPETUBARAO",
            table=f"pims_osisoft_pelletizing_{n_usina}",
        )
        df_lab = self.spark.sql(sql1).select("*")

        # aplica uma condicional...
        if df_lab.count() > 0:
            writeControl = 0
            while writeControl < 5:
                try:
                    df_lab = df_lab.toPandas()
                    break
                except Exception as err:
                    logger.exception(err)
                    time.sleep(10 * writeControl)
                    writeControl = writeControl + 1

            df_lab.ts = pd.to_datetime(df_lab.ts, unit="ms")
            dicTags = dict(zip(taglist.tagdl, taglist.tagpims))
            df_lab.name = df_lab.name.map(dicTags)
            return df_lab
        return None

    def getLabDataSL(self, tagpims, tini, tfim):
        tagdl = self.tagPIMStoDataLake(tagpims)
        taglist = pd.DataFrame({"tagdl": tagdl, "tagpims": tagpims})

        sql1 = sqlModule().geraSQL1(
            tini, tfim, taglist, source="PIMSDIPESAOLUIS"
        )
        df_lab = self.spark.sql(sql1).select("*")

        if df_lab.count() > 0:
            writeControl = 0
            while writeControl < 5:
                try:
                    df_lab = df_lab.toPandas()
                    break
                except Exception as e:
                    time.sleep(10 * writeControl)
                    writeControl += 1

            df_lab.ts = pd.to_datetime(df_lab.ts, unit="ms")
            dicTags = dict(zip(taglist.tagdl, taglist.tagpims))
            df_lab.name = df_lab.name.map(dicTags)

            return df_lab
        return None

    # REF getMinCargaDate - Recupera ult_pred da tabela pel_op2.UltData
    # no teste do PIsystem, var usina OK, vai buscar usina 108
    def getMinCargaDate(self, usina, dbInfo):
        """Capture the last date of the pelletizing process.

        Parameters
        ----------
        usina : int
            Número/sufixo da usina a ser processada.
        dbInfo : tuple
            tupla de informações obtidas a partir da função dbInfo

        Local variables, sql tables
        ----------
        UltData : sql table
            armazena a última data de inserção das tags de NÃO de laboratório
        """
        driver, sqlserver, port, database, user, pswd, prefix = dbInfo

        cnxn = self.cnxn
        cursor = self.cursor
        cursor.execute(
            f"select ult_pred from {prefix}.UltData where idUsina = {usina} "
            f"and idModelo = 0"
        )
        row = cursor.fetchone()
        cursor.commit()
        cnxn.close()

        tz = pytz.timezone("America/Sao_Paulo")
        tfim = datetime.now(TZ_SAO_PAULO).replace(
            microsecond=0, second=0, minute=0
        )
        tini = row[0]
        if row is None:
            tini = datetime(2019, 1, 1, 0, 0, 0)
        tini = tini.replace(tzinfo=tz)

        return tini, tfim, tini

    # == funções para gerar SQL query ==========================================
    def geraSQL1(self, tini, tfim, taglist, source="PIMSDIPETUBARAO"):
        pims = "i_pims_data."
        table = "pims_pelletizing_data"

        tini_ms = int(tini.strftime("%s")) * 1000  # em milissegundos
        tfim_ms = int(tfim.strftime("%s")) * 1000  # em milissegundos

        # Lista com os anos
        anoini = tini.year
        anofim = tfim.year
        yearlist = str(tuple(range(anoini, anofim + 1)))
        yearlist = re.sub(",(\))", r"\1", yearlist)
        #   log.info(yearlist)

        # lista mes
        mesini = tini.month
        mesfim = tfim.month
        if anofim == anoini:
            monthlist = tuple(range(mesini, mesfim + 1))
        else:
            monthlist = tuple(range(1, 13))
        monthlist = [str(x).zfill(2) for x in monthlist]
        auxmonth = ""
        for i in monthlist:
            auxmonth += f"'{i}'"
            if i != monthlist[len(monthlist) - 1]:
                auxmonth += ","

        names_query = ""
        for i in taglist.index:
            names_query = (
                names_query + "name = '" + taglist.loc[i, "tagdl"] + "'"
            )
            if i != taglist.index.max():
                names_query = names_query + " OR "
            # sem virgula na ultima tag
        sql1 = f"""
      SELECT name, time_stamp as ts, value
      FROM {pims}{table}
      WHERE
        ({names_query})
      AND
        time_stamp BETWEEN {str(tini_ms)} AND {str(tfim_ms)}
      -- PARTITION
      AND
        source_system = '{source}'
      AND
        year in {yearlist}
      AND
        month in ({auxmonth})
      """
        return sql1

    def geraSQL_pisystem(
        self,
        tini,
        tfim,
        taglist,
        source="PIMSDIPETUBARAO",
        table="pims_osisoft_pelletizing",
    ):
        """Gera sql query para extrair posteriormente dados do pi_system.

        Parameters
        ----------
        tini : datetime.datetime
            Data/hora de início da extração dos dados de entrada.
        tfim : datetime
            data/hora de fim da extração de dados
        taglist : pd.DataFrame
            de-para entre formato novo do ``pi_system`` e a antiga nomenclatura
            do pims
        source : str
            obsoleto
        table : str
            tabela de extração dos dados

        Local variables
        ----------
        names_query : string
            variável usada na execução de query para restringir (via WHERE)
            as tags selecionadas
        """
        pims = ""  # remover
        # Lista com os Anos referentes ao período de extração
        anoini = tini.year
        anofim = tfim.year
        yearlist = str(tuple(range(anoini, anofim + 1)))
        yearlist = re.sub(",(\))", r"\1", yearlist)

        # lista mes
        mesini = tini.month
        mesfim = tfim.month
        if anofim == anoini:
            auxmonth = tuple(range(mesini, mesfim + 1))
        else:
            auxmonth = tuple(range(1, 13))
        if len(auxmonth) == 1:
            auxmonth = str(auxmonth).replace(",", "")

        names_query = ""
        for i in taglist.index:
            names_query = (
                names_query + "name = '" + taglist.loc[i, "tagdl"] + "'"
            )
            if i != taglist.index.max():
                names_query = names_query + " OR "
            # sem virgula na ultima tag
        sql1 = f"""
      SELECT name, ts, value
      FROM
      (
      SELECT
        DATEADD(HOUR, -3, timestamp) as ts
        , name, value, year, month
      FROM
          {pims}{table}
      ) as tab
      WHERE
       ({names_query})
      AND
        ts BETWEEN '{str(tini)}' AND '{str(tfim)}'
      -- PARTITION
      -- AND
      --   year in {yearlist}
      -- AND
      --   month in {auxmonth}
      """

        return sql1

    def geraSQL2(self, tini, tfim, tag):
        pims = "i_pims_data."
        source = "PIMSDIPETUBARAO"
        table = "pims_pelletizing_data"

        tini_ms = int(tini.strftime("%s")) * 1000  # em milissegundos
        tfim_ms = int(tfim.strftime("%s")) * 1000  # em milissegundos
        tini_str = tini.strftime("%Y-%m-%d %H:%M:%S")  # formato string
        tfim_str = tfim.strftime("%Y-%m-%d %H:%M:%S")  # formato string

        # lista anos
        anoini = int(tini_str.split("-")[0])
        anofim = int(tfim_str.split("-")[0])
        yearlist = (
            str(list(range(anoini, anofim + 1)))
            .replace("[", "")
            .replace("]", "")
        )

        # lista meses
        mesini = int(tini_str.split("-")[1])
        mesfim = int(tfim_str.split("-")[1])
        if anofim == anoini:
            monthlist = list(range(mesini, mesfim + 1))
        else:
            monthlist = list(range(1, 13))
        monthlist = [str(x).zfill(2) for x in monthlist]
        auxmonth = ""
        for i in monthlist:
            auxmonth = auxmonth + "'" + str(i) + "'"
            if i != monthlist[len(monthlist) - 1]:
                auxmonth = auxmonth + ","

        # SELECT
        return f"""
        SELECT NAME,
               time_stamp AS ts,
               value
          FROM {pims}{table}
         WHERE NAME = '{tag}'
           AND time_stamp BETWEEN '{str(tini_ms)}' AND '{str(tfim_ms)}'
           AND source_system = '{source}'
           AND year IN ({yearlist})
           AND month IN ({auxmonth})
        """

    def geraSQL3(self, tini, tfim, taglist):
        pims = "i_pims_data."
        source = "PIMSDIPETUBARAO"
        table = "pims_pelletizing_data"

        tini_ms = int(tini.strftime("%s")) * 1000  # em milisegundos
        tfim_ms = int(tfim.strftime("%s")) * 1000  # em milisegundos
        tini_str = tini.strftime("%Y-%m-%d %H:%M:%S")  # formato string
        tfim_str = tfim.strftime("%Y-%m-%d %H:%M:%S")  # formato string

        # lista anos
        anoini = int(tini_str.split("-")[0])
        anofim = int(tfim_str.split("-")[0])
        yearlist = (
            str(list(range(anoini, anofim + 1)))
            .replace("[", "")
            .replace("]", "")
        )

        # lista mes
        mesini = int(tini_str.split("-")[1])
        mesfim = int(tfim_str.split("-")[1])
        if anofim == anoini:
            monthlist = list(range(mesini, mesfim + 1))
        else:
            monthlist = list(range(1, 13))
        monthlist = [str(x).zfill(2) for x in monthlist]
        auxmonth = ""
        for i in monthlist:
            auxmonth += f"'{i}'"
            if i != monthlist[len(monthlist) - 1]:
                auxmonth += ","

        # SELECT
        sql1 = "SELECT name, time_stamp as ts, value_str "
        # FROM
        sql1 += "FROM " + pims + table + " "
        # WHERE
        sql1 += "WHERE( "
        for i in taglist.index:
            sql1 += "name = '" + taglist.loc[i, "tagdl"] + "'"
            if i != taglist.index.max():
                sql1 += " or "  # sem virgula
            # na ultima tag
        sql1 += ") AND "
        sql1 = (
            sql1 + "time_stamp BETWEEN " + str(tini_ms) + " AND " + str(tfim_ms)
        )

        # PARTITION
        sql1 = (
            sql1 + " AND " + "source_system = '" + source + "' AND year in "
            "(" + yearlist + ")"
        )
        sql1 += " AND " + "month in (" + auxmonth + ")"
        return sql1

    def getFVarReport(self, tini, tfim, taglist):
        #   import pyodbc
        cnxn = self.cnxn
        cursor = self.cursor

        # SELECT
        sql1 = "SELECT data, variavel, valor "
        # FROM
        sql1 += f"FROM {self.prefix}.fVarpims "
        # WHERE
        sql1 += "WHERE( "
        for i in range(0, len(taglist)):
            sql1 += "variavel = '" + taglist[i] + "'"
            if i != len(taglist) - 1:
                sql1 += " or "  # sem virgula na ultima tag
        sql1 += ") AND "
        sql1 += ("data BETWEEN '{}' AND '{:%Y-%m-%d %H:%M:00}'").format(
            tini, tfim
        )
        cursor.execute(sql1)
        row = cursor.fetchall()
        cursor.commit()
        cnxn.close()
        return pd.DataFrame.from_records(
            row, columns=[column[0] for column in cursor.description]
        )

    # usado para checar SQL
    # no teste do PIsystem, var usina está errado, vai buscar usina 108 numa
    # tabela que so tem Usina 8
    def getFVarParam(
        self, usina, tini, tfim, taglist, fimFormat=True, param=""
    ):
        cnxn = self.cnxn
        cursor = self.cursor

        vars_query = ""
        for i in taglist.index:
            vars_query = (
                vars_query + f"variavel = '" f"{taglist.loc[i, 'tagpims']}'"
            )
            if i != taglist.index.max():
                vars_query = vars_query + " OR "  #
            # sem virgula na ultima tag
        sql1 = f"""
      SELECT
          data, variavel, valor
      FROM
          {prefix}.fVarpims_{param}
      WHERE
          ( {vars_query} )
      AND
          idUsina = {str(usina)}
      AND
          """
        if fimFormat:
            sql1 += (
                "data BETWEEN '{}' AND '{:%Y-%m-%d %H:%M:00}'"
            ).format(tini, tfim)
        else:
            sql1 += f"data BETWEEN '{tini}' AND '{tfim}'"

        cursor.execute(sql1)
        row = cursor.fetchall()
        cursor.commit()
        cnxn.close()
        return pd.DataFrame.from_records(
            row, columns=[column[0] for column in cursor.description]
        )

    # # copiado de utils. É usado nos antigos compressao_model e similares
    # # Verifica se tag em analise tem dados no df. Caso nao tenha, consulta
    # mais 3 dias para tras de tini, pega o ultimo valor historico dela e
    # coloca como valor dessa tag para todos os registros do df
    # def getTAGsDelay(self):
    #   lTasgs = list(set(tagpims) - set(df.columns))
    #   for tagCheck in lTasgs:
    #     taglistCheck = taglist[taglist['tagpims'] == tagCheck]
    #     try:
    #       df[dicTags[tagCheck]].head()
    #     except:
    #       log.info('Recuperando TAG: {}'.format(tagCheck))
    #       tiniCheck = tini - datetime.timedelta(hours=1000)
    #       dfCheck = self.spark.sql(sqlModule().geraSQL1(tiniCheck, tfim,
    #       taglistCheck)).select("*")
    #       if dfCheck.count() > 0:
    #         dfCheck = dfCheck.toPandas() #puxa dados e converte df spark
    #         para df pandas
    #         dfCheck.ts = pd.to_datetime(dfCheck.ts, unit='ms')
    #         df[dicTags[dfCheck.name[0]]] = dfCheck[dfCheck.ts == max(
    #         dfCheck.ts)]['value'].values[0]
    #       else:
    #         log.debug("Nenhum dado foi recuperado")

    # REF getTAGsDelay - Verifica se TAG em análise tem dados no df. Caso
    # nao tenha, consulta mais 3 dias para trás de tini, pega o último valor
    # histórico dela e coloca como valor dessa tag para todos os registros
    # do df
    def getTAGsDelay(self, df, tini, tfim, tagpims, taglist):
        dicTags = dict(zip(taglist.tagdl, taglist.tagpims))
        lTasgs = list(set(tagpims) - set(df.columns))

        for tagCheck in lTasgs:
            taglistCheck = taglist[taglist["tagpims"] == tagCheck]
            try:
                df[dicTags[tagCheck]].shape
            except:
                tiniCheck = tini - timedelta(hours=80)
                dfCheck = self.spark.sql(
                    sqlModule().geraSQL3(tiniCheck, tfim, taglistCheck)
                ).select("*")
                if dfCheck.count() > 0:
                    dfCheck = dfCheck.toPandas()  # puxa dados e converte df
                    # spark para df pandas
                    dfCheck.ts = pd.to_datetime(dfCheck.ts, unit="ms")

                    dfCheck = dfCheck.fillna(99999)
                    dfCheck.value_str = dfCheck.value_str.apply(
                        lambda x: str(x)
                    ).replace("???????", "99999")
                    dfCheck.value_str = dfCheck.value_str.apply(
                        lambda x: str(x)
                    ).replace("None", "99999")
                    dfCheck.value_str = dfCheck.value_str.apply(
                        lambda x: str(x).strip()
                    )
                    dfCheck.loc[
                        ~dfCheck.value_str.str.match(
                            "^[-+]?(\d+([.,]\d*)?|[.,]\d+)([eE][-+]?\d+)?$"
                        ),
                        "value_str",
                    ] = "99999"
                    dfCheck.value_str = dfCheck.value_str.apply(
                        lambda x: float(x)
                    )
                    dfCheck = dfCheck.rename(columns={"value_str": "value"})

                    df[dicTags[dfCheck.name[0]]] = dfCheck[
                        dfCheck.ts == max(dfCheck.ts)
                    ]["value"].values[0]
                else:
                    df[tagCheck] = 0
        return df

    # getLastValueTAGs_v2 -- Recupera o ultimo valor valido para a diferença
    # de tagpims e df.columns
    def getLastValueTAGs_v2(self, df, tagpims, maxHours):

        lTasgs = list(set(tagpims) - set(df.columns))
        tagdl2 = self.tagPIMStoDataLake(lTasgs)
        taglist2 = pd.DataFrame({"tagdl": tagdl2, "tagpims": lTasgs})

        sql1 = sqlModule().geraSQL1(
            (tini - datetime.timedelta(hours=maxHours)), tfim, taglist2
        )  # gera query
        df2 = self.spark.sql(sql1)  # puxa dados

        writeControl = 1
        while writeControl < 5:
            try:
                df2 = df2.toPandas()  # puxa dados e converte df spark para
                # df pandas
                break
            except Exception as e:
                logger.debug(f"Sleep for {writeControl * 20} seconds...")
                time.sleep(20 * writeControl)
                writeControl += 1

        df2.ts = pd.to_datetime(df2.ts, unit="ms")

        dicTags = dict(zip(taglist2.tagdl, taglist2.tagpims))
        df2.name = df2.name.map(dicTags)
        df2.rename(
            columns={"name": "variavel", "value": "valor", "ts": "data"},
            inplace=True,
        )
        df2 = df2.pivot(index="data", columns="variavel", values="valor")
        df2 = df2.fillna(method="ffill").fillna(method="bfill")

        df3 = pd.concat([df, df2], axis=1)

        return df3.loc[df3.index.isin(df.index)]

    # getLastValueTAGs -- Recupera o ultimo valor valido para a diferenca de
    # tagpims e df.columns
    def getLastValueTAGs(self, df, tagpims, maxHours):
        lTasgs = list(set(tagpims) - set(df.columns))
        for tagCheck in lTasgs:
            taglistCheck = taglist[taglist["tagpims"] == tagCheck]
            try:
                df[dicTags[tagCheck]].shape
            except:
                print("Recuperando TAG: {}".format(tagCheck))
                tiniCheck = tini - datetime.timedelta(hours=maxHours)
                dfCheck = self.spark.sql(
                    sqlModule().geraSQL1(tiniCheck, tfim, taglistCheck)
                ).select("*")
                if dfCheck.count() > 0:
                    dfCheck = dfCheck.toPandas()  # puxa dados e converte df
                    # spark para df pandas
                    dfCheck.ts = pd.to_datetime(dfCheck.ts, unit="ms")
                    df[dicTags[dfCheck.name[0]]] = dfCheck[
                        dfCheck.ts == max(dfCheck.ts)
                    ]["value"].values[0]
                else:
                    print("Nenhum dado foi recuperado")
                    df[tagCheck] = 0
        return df

    def tagPIMStoDataLake_v1(self, tagList):
        return [
            t.replace("@", "_")
            .replace(".", "_")
            .replace(",", "_")
            .replace("-", "_")
            .replace("__", "_ME")
            .replace("+", "MA")
            + "__3600"
            for t in tagList
        ]

    # tagPIMStoDataLake_v1 — Versão antiga que fazia a conversao das TAGs do
    # formato do PIMs para o formato utilizado no DATALAKE
    def tagPIMStoDataLake(self, tagList):
        return [
            re.sub(
                "[^a-zA-Z0-9_]",
                "_",
                t.replace("_-", "_ME")
                .replace("_+", "_MA")
                .replace("-ANLG", ""),
            ).upper()
            + "__3600"
            for t in tagList
        ]

    # tagPIMStoDataLake - Convert uma lista de TAGs no formato do PIMs para
    # o formato utilizado no DATALAKE
    # ANLG é uma sintaxe existente em algumas poucas tags de VGR, mas que nao
    # existe no datalake e deve ser, portanto removido.

    # versão trazida do utils
    # # Convert uma lista de TAGs no formato do PIMs para o formato utilizado
    # no DATALAKE
    # def tagPIMStoDataLake(self, tagList):
    #   return [t.replace('@', '_').replace(',', '_').replace('-',
    #   '_').replace('__', '_ME').replace('+', 'MA')+'__3600' for t in tagList]

    # data_quality - Verificar a qualidade dos dados com relação a `NaN` e `inf`
    def data_quality(self, df):
        return pd.DataFrame(
            [
                [
                    i,  # Nome do atributo
                    df[i].isna().sum(),  # Quantidade de NaN
                    (df[i] == np.inf).sum(),  # Quantidade de +inf
                    (df[i] == -np.inf).sum(),
                ]
                for i in df.columns
            ],
            # Quantidade de -inf
            columns=["Atributo", "NaN", "+inf", "-inf"],
        ).sort_values(by=["NaN", "+inf", "-inf"], ascending=False)

    # getFVarMaxDate -- Recupera idUsina_modelo da tabela DUsina_modelo
    def getFVarMaxDate(self, idTAG, usina_modelo, operator):
        cnxn = self.cnxn
        cursor = self.cursor

        if operator == "==":
            cursor.execute(
                f"select MAX(data) from {self.prefix}.FVar where idVariavel = "
                f"{idTAG} and idUsina_modelo = {usina_modelo}"
            )
        else:
            cursor.execute(
                f"SELECT MAX(data) from {self.prefix}.FVar where idVariavel "
                f"<> {idTAG} and idUsina_modelo"
                f" = {usina_modelo}"
            )
        row = cursor.fetchone()
        cursor.commit()
        cnxn.close()
        return row

    # disco -- retorna string contendo "disco" + numero do disco em formato 0
    def disco(self, num):
        """
        Return a string with the disk number in the format 0#.

        For example: `'disco01'`
        """
        return f"disco{str(num).zfill(2)}"

    # funções de formatação da variável
    def format_key(self, k):
        if "_" in k:
            return "".join(s[0].upper() for s in k.split("_"))
        return k[:4]

    def format_value(self, v):
        if v is True:
            return "Y"
        if v is False:
            return "N"
        if v is None:
            return "-"
        return v

    def params_to_str(self, params):
        return ";".join(
            "{}={}".format(format_key(k), format_value(v))
            for k, v in params.items()
            if k not in {"random_state", "seed"}
        )

    # completarTabela -- formatacao de dataframe que vai escrever no banco SQL
    def completarTabela(self, dfVAR, dicTarget, dicParametros):
        # Coloca nome original da TAG
        dfVAR["target"] = 1
        dfVAR["name"] = dfVAR["name"].map(dicTags)
        dfVAR["idUsina"] = dicParametros["usina"]
        dfVAR["idModelo"] = dicParametros["modelo"]
        dfVAR["idUsina_modelo"] = dicParametros["usina_modelo"]
        dfVAR["idVariavel"] = [dicTarget for x in dfVAR["name"]]
        dfVAR["data"] = pd.to_datetime(dfVAR["ts"], unit="ms")
        dfVAR.rename(columns={"value": "valor"}, inplace=True)
        seqVar = getSeq("pel_op.FVar", "idFVar")
        dfVAR["idFVar"] = list(range(seqVar, seqVar + dfVAR.shape[0]))

        cols = ["idFVar", "idModelo", "idUsina", "idUsina_modelo",
                "idVariavel", "data", "valor", "target"]
        dfVAR = dfVAR[cols]
        dfVAR.sort_values(by="data", ascending=True, inplace=True)
        return dfVAR

    # trataTabela -- inicializa dataframe e formata datas de inicio e fim
    def trataTabela(self, modelo, tabDatas, dicTarget, dicParametros):
        dfVAR = pd.DataFrame()
        tini = tabDatas.loc[modelo, "ult_dt_target"] + datetime.timedelta(
            hours=1
        )
        tfim = tabDatas.loc[modelo, "ult_dt_var"]
        sql2 = sqlModule().geraSQL2(
            tini, tfim, tabDatas.loc[modelo, "targetdl"]
        )  # puxa dados e converte df spark para df
        # pandas
        dfVAR = self.spark.sql(sql2).select("*").toPandas()
        dfVAR = completarTabela(dfVAR, dicTarget, dicParametros)
        print(modelo, dfVAR["data"].min(), dfVAR["data"].max())
        return dfVAR

    # salvaPred - Grava os resultados dos modelos de predição banco de dados
    def salvaPred(self, df_pred):
        writeControl = 1
        while writeControl < 5:
            try:
                (
                   self.spark.createDataFrame(df_pred)
                   .write.format("jdbc")
                   .option(
                       "url",
                       f"jdbc:sqlserver://{self.sqlserver}:1433;databaseName={self.database}",
                   )
                   .option("dbtable", f"{self.prefix}.FPred")
                   .option("user", self.user)
                   .option("password", self.pswd)
                   .mode("append")
                   .save()
               )
                logger.info(
                    f"Inserido em pel_op.FPred: {df_pred.shape[0]:,} linhas"
                )
                break
            except Exception as err:
                print(err)
                time.sleep(5 * writeControl)
                writeControl += 1
                print("sleep 5 seconds")

    # salvaUltData - grava ultima data de predição para o modelo
    # no teste do PIsystem, var usina OK, vai buscar usina 108
    def salvaUltData(self, ult_pred, usina, modelo, ultima_previsao):
        """salva última data/hora de inserção das tags na tabela UltData

        Sobre as condicionais if/else
        ----------
        if ultima_previsao == (datetime(2019, 1, 1, 0, 0, 0)):
            insere última data
        else:
            faz update da última data

        Parameters
        ----------
        ult_pred : datetime.datetime
            última data da carga a inserir na tabela
            Input data.
        usina : int


        Local variables, sql tables
        ----------
        UltDataTag : sql table
            armazena a última data de inserção das tags de laboratório
        sqlVar : string
            variável usada na execução de query para restringir (via WHERE)
            as tags selecionadas
        """
        cnxn = self.cnxn
        cursor = self.cursor

        if ultima_previsao == (datetime(2019, 1, 1, 0, 0, 0)):
            cursor.execute(
                f"insert into {self.prefix}.UltData (ult_pred, idUsina, "
                f"idModelo) values ('{ult_pred}', {usina}, {modelo})"
            )
        else:
            cursor.execute(
                f"update {self.prefix}.UltData set ult_pred = '{ult_pred}' where "
                f"idUsina = {usina} and idModelo = {modelo}"
            )
        cursor.commit()
        cnxn.close()

    # salvaTarget -- Salvar target no banco
    def salvaTarget(self, dfVAR):
        (
            self.spark.createDataFrame(dfVAR)
            .write.format("jdbc")
            .option(
                "url",
                f"jdbc:sqlserver://{self.sqlserver}:1433;databaseName"
                f"={self.database}",
            )
            .option("dbtable", f"{self.prefix}.FVar")
            .option("user", self.user)
            .option("password", self.pswd)
            .mode("append")
            .save()
        )

    def checkdf(self, dfVAR):
        """Verify if dataframe is empty.

        Parameters
        ----------
        dfVAR : pd.DataFrame
            Pandas DataFrame with data to be verified.

        Returns
        -------
        bool
            ``True`` if :param:`dfVAR` is not empty, ``False`` otherwise.
        """
        return not dfVAR.empty

    # buscaIDVariavel - busca ID da variável no banco SQL
    def buscaIDVariavel(self, x, df_DVariavel, dicParametros):
        tmp = df_DVariavel.copy()
        tmp = tmp[tmp["idUsina_modelo"] == dicParametros["usina_modelo"]]
        idVariavel = tmp.loc[tmp["descricao"] == x, "idVariavel"]
        if len(idVariavel) > 0:
            return idVariavel.values[0]
        return x

    # buscaIDVariavel2 -- busca ID da variaval no banco SQL
    def buscaIDVariavel2(self, x, df_DVariavel):
        idVariavel = df_DVariavel.loc[
            df_DVariavel["descricao"] == x, "idVariavel"
        ]
        if len(idVariavel) > 0:
            return idVariavel.values[0]
        return x

    # getIDVariavel -- Recupera idUsina_modelo da tabela DUsina_modelo
    def getIDVariavel(self, usina_modelo):
        cnxn = self.cnxn
        cursor = self.cursor
        cursor.execute(
            f"SELECT idVariavel, descricao FROM {self.prefix}.DVariavel "
            f" WHERE idUsina_modelo={usina_modelo}"
        )
        row = cursor.fetchall()
        cursor.commit()
        cnxn.close()
        return row
