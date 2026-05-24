from pyspark.sql import SparkSession
from pyspark.sql import functions as F

pasta_raiz = "C:/Users/vinil/Documents/GitHub/BigData"
pasta_lote = f"{pasta_raiz}/vistas_lote_spark"
pasta_vtr = f"{pasta_raiz}/vistas_velocidade_spark"

spark = SparkSession.builder \
    .appName("CamadaApresentacao") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

# ============================================================
# CARREGAR VISTAS DE LOTE
# ============================================================
lote_vendas_dia = spark.read.option("header","true").option("inferSchema","true").csv(f"{pasta_lote}/vendas_por_dia")
lote_acessos_pais = spark.read.option("header","true").option("inferSchema","true").csv(f"{pasta_lote}/acessos_por_pais")
lote_vendas_metodo = spark.read.option("header","true").option("inferSchema","true").csv(f"{pasta_lote}/vendas_por_metodo")
lote_duracao_browser = spark.read.option("header","true").option("inferSchema","true").csv(f"{pasta_lote}/duracao_por_browser")

# ============================================================
# CARREGAR VISTAS DE VELOCIDADE
# ============================================================
vtr_vendas_dia = spark.read.option("header","true").option("inferSchema","true").csv(f"{pasta_vtr}/vendas_por_dia/resultado")
vtr_acessos_pais = spark.read.option("header","true").option("inferSchema","true").csv(f"{pasta_vtr}/acessos_por_pais/resultado")
vtr_vendas_metodo = spark.read.option("header","true").option("inferSchema","true").csv(f"{pasta_vtr}/vendas_por_metodo/resultado")
vtr_duracao_browser = spark.read.option("header","true").option("inferSchema","true").csv(f"{pasta_vtr}/duracao_por_browser/resultado")

# ============================================================
# CONSULTA 1 — VENDAS POR DIA
# ============================================================
combinado_vendas_dia = lote_vendas_dia.select("data","total_vendas") \
    .union(vtr_vendas_dia.select("data","total_vendas")) \
    .groupBy("data") \
    .agg(F.sum("total_vendas").alias("total_vendas")) \
    .orderBy("data")

print("=== Vendas por Dia (completo) ===")
combinado_vendas_dia.show()

# ============================================================
# CONSULTA 2 — ACESSOS POR PAÍS
# ============================================================
combinado_acessos_pais = lote_acessos_pais \
    .union(vtr_acessos_pais) \
    .groupBy("country") \
    .agg(F.sum("total_acessos").alias("total_acessos")) \
    .orderBy(F.desc("total_acessos"))

print("=== Acessos por País (completo) ===")
combinado_acessos_pais.show()

# ============================================================
# CONSULTA 3 — VENDAS POR MÉTODO
# ============================================================
combinado_vendas_metodo = lote_vendas_metodo \
    .union(vtr_vendas_metodo) \
    .groupBy("pay_method") \
    .agg(
        F.sum("total_vendas").alias("total_vendas"),
        F.sum("contagem").alias("contagem")
    ) \
    .orderBy(F.desc("total_vendas"))

print("=== Vendas por Método (completo) ===")
combinado_vendas_metodo.show()

# ============================================================
# CONSULTA 4 — DURAÇÃO MÉDIA POR BROWSER
# Média ponderada: soma_total / contagem_total
# ============================================================
lote_dur = lote_duracao_browser \
    .withColumn("soma_duracao", F.col("media_duracao") * F.lit(1)) \
    .withColumn("contagem", F.lit(1))

combinado_duracao = lote_dur.select("accessed_Ffom","soma_duracao","contagem") \
    .union(vtr_duracao_browser.select("accessed_Ffom","soma_duracao","contagem")) \
    .groupBy("accessed_Ffom") \
    .agg(
        (F.sum("soma_duracao") / F.sum("contagem")).alias("media_duracao_combinada")
    ) \
    .orderBy(F.desc("media_duracao_combinada"))

print("=== Duração Média por Browser (completo) ===")
combinado_duracao.show()

spark.stop()