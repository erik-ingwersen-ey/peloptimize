# Databricks notebook source
# MAGIC %md
# MAGIC # instalações necessárias sempre que reseta o cluster

# COMMAND ----------

#%pip install pulp

# COMMAND ----------

#%md
# fsspec - erro novo
#%pip install fsspec

# COMMAND ----------

#%md
## combo
#%pip install combo

# COMMAND ----------

# MAGIC %md
# MAGIC ##odbc driver v17
# MAGIC necessário caso ocorra erro que exija tal instalação
# MAGIC * Isso acontece para versões >= numpy1.18.5

# COMMAND ----------

# MAGIC %md
# MAGIC #### checa odbc driver
# MAGIC * comando de terminal "odbcinst -j" precisa printar:
# MAGIC   * $ unixODBC 2.3.7
# MAGIC * Caso isso não ocorra, deverá rodar os comandos desta seção

# COMMAND ----------

# MAGIC %sh
# MAGIC odbcinst -j

# COMMAND ----------

# MAGIC %md
# MAGIC #### instala driver odbc 7

# COMMAND ----------

# MAGIC %sh
# MAGIC if ! [[ "16.04 18.04 20.04 21.04 21.10" == *"$(lsb_release -rs)"* ]];
# MAGIC then
# MAGIC     echo "Ubuntu $(lsb_release -rs) is not currently supported.";
# MAGIC     exit;
# MAGIC fi
# MAGIC
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc
# MAGIC sudo apt-key add microsoft.asc
# MAGIC
# MAGIC curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC
# MAGIC exit

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get update
# MAGIC sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB3E94ADBE1229CF
# MAGIC sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
# MAGIC # optional: for bcp and sqlcmd
# MAGIC sudo ACCEPT_EULA=Y apt-get install -y mssql-tools
# MAGIC echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
# MAGIC source ~/.bashrc
# MAGIC # optional: for unixODBC development headers
# MAGIC sudo apt-get install -y unixodbc-dev

# COMMAND ----------

# MAGIC %md
# MAGIC #### checa novamente o driver

# COMMAND ----------

# MAGIC %sh
# MAGIC odbcinst -j

# COMMAND ----------

# MAGIC %md
# MAGIC #outros

# COMMAND ----------

# MAGIC %md
# MAGIC ##databricks command line interface (obs: esta travando)
# MAGIC * instruções sobre CLI (seção 2): https://blog.getcensus.com/4-ways-to-export-csv-files-from-databricks/
# MAGIC * para gerar novo token: https://docs.databricks.com/dev-tools/api/latest/authentication.html

# COMMAND ----------

#%sh
#pip install databricks-cli

# COMMAND ----------

#%sh
#databricks configure --token

# COMMAND ----------

