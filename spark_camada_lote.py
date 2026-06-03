from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

pasta_raiz = "C:/Users/vinil/Documents/GitHub/BigData"
pasta_dados = f"{pasta_raiz}/dados_brutos/**/*.csv"
pasta_saida = f"{pasta_raiz}/vistas_lote_spark"

# Cria a SparkSession
spark = SparkSession.builder \
    .appName("CamadaLote") \
    .getOrCreate()

# Reduz logs desnecessários no terminal
spark.sparkContext.setLogLevel("ERROR")

# LEITURA DOS DADOS PARTICIONADOS
# O Spark lê recursivamente toda a pasta dados_brutos/
# inferSchema detecta automaticamente os tipos de cada coluna
# leitura recursiva pois os arquivos estão em subpastas por data
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("recursiveFileLookup", "true") \
    .option("pathGlobFilter", "*.csv") \
    .csv(f"{pasta_raiz}/dados_brutos")

# Converte a coluna de data para o tipo correto
df = df.withColumn("accessed_date", F.to_timestamp("accessed_date"))

print(f"Total de registros carregados: {df.count()}")
df.show(5)

# VISTA 1 — VENDAS POR DIA
# Extrai só a data (sem hora) e agrega soma e média de sales por dia
vendas_por_dia = df \
    .withColumn("data", F.to_date("accessed_date")) \
    .groupBy("data") \
    .agg(
        F.sum("sales").alias("total_vendas"),
        F.count("sales").alias("contagem")
    ) \
    .orderBy("data")

vendas_por_dia.show()
vendas_por_dia.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{pasta_saida}/vendas_por_dia")

# VISTA 2 — ACESSOS POR PAÍS
# Conta o número de acessos por país de origem
acessos_por_pais = df \
    .groupBy("country") \
    .agg(F.count("ip").alias("total_acessos")) \
    .orderBy(F.desc("total_acessos"))

acessos_por_pais.show()
acessos_por_pais.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{pasta_saida}/acessos_por_pais")

# VISTA 3 — DURAÇÃO MÉDIA POR BROWSER
# spark_camada_lote.py — corrigir para salvar os componentes
duracao_por_browser = df \
    .groupBy("accessed_Ffom") \
    .agg(
        F.sum("duration_(secs)").alias("soma_duracao"),
        F.count("duration_(secs)").alias("contagem")
    ) \
    .orderBy(F.desc("soma_duracao"))

duracao_por_browser.show()
duracao_por_browser.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{pasta_saida}/duracao_por_browser")

# VISTA 4 — VENDAS POR MÉTODO DE PAGAMENTO
# Soma total e contagem de vendas por método
vendas_por_metodo = df \
    .groupBy("pay_method") \
    .agg(
        F.sum("sales").alias("total_vendas"),
        F.count("sales").alias("contagem")
    ) \
    .orderBy(F.desc("total_vendas"))

vendas_por_metodo.show()
vendas_por_metodo.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{pasta_saida}/vendas_por_metodo")

print("Vistas de lote geradas em:", pasta_saida)
spark.stop()