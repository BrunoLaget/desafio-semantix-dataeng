# Databricks notebook source
############################################################
# DESAFIO ENGENHEIRO DE DADOS - SEMANTIX - V1.0            #
# Autor: BRUNO LAGET MERINO                                #
# Contato: brunolagetm@hotmail.de                          #
# Data: 09/08/2019                                         #
############################################################

# COMMAND ----------

######################################################
# IMPORT DE BIBLIOTECAS
# Escolhi escrever o codigo do teste em Python, utilizando
#portanto o Pyspark
import pyspark
#import de funções para data cleaning
from  pyspark.sql.functions import regexp_replace, substring, length, col, expr

# COMMAND ----------

######################################################
#UPLOAD DOS CONJUNTOS DE DADOS:
# Arquivo de Ago95 do link do teste possuia 15,6mb, nao 21,8. O tamanho dos arquivos 
#apos descompactacao tambem nao bate com o enunciado. Total de linhas dos 2 arquivos
#somados: 3461612.
#
# Os dois conjuntos de dados foram subidos no Databricks Notebook via UI do notebook, 
#sendo portanto gravados como um dataframe. O separador usado foi o espaco (" "). 
#
# Foi utilizado o notebook versao Community. O upload dos arquivos diretos apresentou
#erro, portanto eu dividi os arquivos em blocos de 500000-600000 linhas, subindo cada
#bloco individualmente. Foram criadas dessa forma 6 tabelas, "tabela01","tabela02",...
#

# COMMAND ----------

######################################################
# CARGA DAS TABELAS NO DATAFRAME PRINCIPAL
#Carrega a primeira tabela em um dataframe
dataframe = spark.table("tabela01")
#Utiliza um segundo dataframe temporario
#para juntar as demais partes
for i in range(2,7):
  tablename = "tabela0"+str(i)
  dataframejoin = spark.table(tablename)
  dataframe = dataframe.unionAll(dataframejoin)

# COMMAND ----------

#Contagem de verificação. Deve somar 3461612 se correto.
#dataframe.count()

# COMMAND ----------

######################################################
# DATA CLEANING
#Excluidas duas colunas sem dados que foram criadas ao
#usar espaço como delimitador
dataframe = dataframe.drop("_c1")
dataframe = dataframe.drop("_c2")
#rename das colunas para nomes mais amigáveis
dataframe = dataframe.withColumnRenamed("_c0","requisitionHost")
dataframe = dataframe.withColumnRenamed("_c3","time")
dataframe = dataframe.withColumnRenamed("_c4","timezone")
dataframe = dataframe.withColumnRenamed("_c5","requisition")
dataframe = dataframe.withColumnRenamed("_c6","httpcode")
dataframe = dataframe.withColumnRenamed("_c7","bytesreturned")
#ajuste da coluna time, deixando apenas a data
dataframe = dataframe.withColumn('time', regexp_replace('time', '\\[', ''))
dataframe = dataframe.withColumn("time",expr("substring(time, 1, length(time)-9)"))
#dataframe = dataframe.withColumn('time', regexp_replace('time', '\\:.', ''))
#ajuste da coluna timezone
dataframe = dataframe.withColumn('timezone', regexp_replace('timezone', '\\]', ''))
dataframe = dataframe.withColumn('timezone', regexp_replace('timezone', '-', ''))
#substitui - por 0 na ultima coluna
dataframe = dataframe.withColumn('bytesreturned', regexp_replace('bytesreturned', '-', '0'))
#Altera o tipo da coluna bytesreturned para int
dataframe = dataframe.withColumn("bytesreturned", dataframe["bytesreturned"].cast("integer"))

#exclui as linhas vazias
#dframe = dframe.na.drop()
#exibe o dataframe para verificacao
dataframe.show()

# COMMAND ----------

######################################################
# QUESTOES DO DESAFIO
# 1. Numero de hosts Unicos.
dataframe.select("requisitionHost").distinct().count()


# COMMAND ----------

#2. O total de erros 404.
dataframe.filter(dataframe["httpcode"]=="404").count()

# COMMAND ----------

#3. Os 5 URLs que mais causaram erro 404
#dataframe.filter(dataframe["httpcode"]=="404").agg(countDistinct("requisitionHost"))
dataframe.filter(dataframe["httpcode"]=="404").groupBy("requisitionHost").count().sort(col('count').desc()).show(5)

# COMMAND ----------

#4. Quantidade de erros 404 por dia
dataframe.filter(dataframe["httpcode"]=="404").groupBy("time").count().sort(col('time')).show(61)

# COMMAND ----------

#5. O total de bytes retornados.
dataframe.groupBy().sum().show()

# COMMAND ----------


