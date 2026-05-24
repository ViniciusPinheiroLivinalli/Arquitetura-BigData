# $env:HADOOP_HOME = "C:\hadoop"
# $env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
# python spark_camada_velocidade.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os

pasta_raiz = "C:/Users/vinil/Documents/GitHub/BigData"
pasta_micro_lotes = f"{pasta_raiz}/dados_novos/micro_lotes"
pasta_saida = f"{pasta_raiz}/vistas_velocidade_spark"
pasta_checkpoint = f"{pasta_raiz}/dados_novos/checkpoint"

spark = SparkSession.builder \
    .appName("CamadaVelocidade") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")

# Schema explícito — obrigatório para readStream
schema = StructType([
    StructField("accessed_date", StringType()),
    StructField("duration_(secs)", IntegerType()),
    StructField("network_protocol", StringType()),
    StructField("ip", StringType()),
    StructField("bytes", IntegerType()),
    StructField("accessed_Ffom", StringType()),
    StructField("age", StringType()),
    StructField("gender", StringType()),
    StructField("country", StringType()),
    StructField("membership", StringType()),
    StructField("language", StringType()),
    StructField("sales", DoubleType()),
    StructField("returned", StringType()),
    StructField("returned_amount", DoubleType()),
    StructField("pay_method", StringType()),
])

# readStream monitora a pasta e processa cada novo arquivo como micro-lote
df_stream = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv(pasta_micro_lotes)

# ============================================================
# VISTAS DE TEMPO REAL
# ============================================================

# Vista 1 — Acessos por país
acessos_pais = df_stream \
    .groupBy("country") \
    .agg(F.count("ip").alias("total_acessos"))

# Vista 2 — Vendas por dia
vendas_dia = df_stream \
    .withColumn("data", F.to_date("accessed_date")) \
    .groupBy("data") \
    .agg(
        F.sum("sales").alias("total_vendas"),
        F.count("sales").alias("contagem")
    )

# Vista 3 — Vendas por método
vendas_metodo = df_stream \
    .groupBy("pay_method") \
    .agg(
        F.sum("sales").alias("total_vendas"),
        F.count("sales").alias("contagem")
    )

# Vista 4 — Duração por browser
duracao_browser = df_stream \
    .groupBy("accessed_Ffom") \
    .agg(
        F.sum("duration_(secs)").alias("soma_duracao"),
        F.count("duration_(secs)").alias("contagem")
    )

# ============================================================
# SAÍDA — modo complete reescreve o resultado acumulado a cada micro-lote
# ============================================================
def iniciar_query(df, nome):
    pasta = f"{pasta_saida}/{nome}"
    os.makedirs(pasta, exist_ok=True)

    def processar_lote(df_lote, id_lote):
        if df_lote.count() > 0:
            df_lote.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(f"{pasta}/resultado")
            print(f"[{nome}] Micro-lote {id_lote} processado.")

    return df.writeStream \
        .outputMode("complete") \
        .foreachBatch(processar_lote) \
        .option("checkpointLocation", f"{pasta_checkpoint}/{nome}") \
        .trigger(processingTime="5 seconds") \
        .start()

q1 = iniciar_query(acessos_pais, "acessos_por_pais")
q2 = iniciar_query(vendas_dia, "vendas_por_dia")
q3 = iniciar_query(vendas_metodo, "vendas_por_metodo")
q4 = iniciar_query(duracao_browser, "duracao_por_browser")

print("Streaming iniciado! Aguardando micro-lotes...")
print("Pressione Ctrl+C para encerrar.")

spark.streams.awaitAnyTermination()