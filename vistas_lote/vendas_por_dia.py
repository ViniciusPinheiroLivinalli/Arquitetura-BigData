import pandas as pd
from pathlib import Path
import os
from collections import defaultdict

pasta_raiz = Path(__file__).parent.parent  # Caminho para a raiz do projeto
pasta_dados = pasta_raiz / "dados_brutos"

# Usar dicionários para acumular soma e contagem por data
sum_sales = defaultdict(float)
count = defaultdict(int)

for arquivo in pasta_dados.rglob("dados.csv"):  # rglob busca recursivamente
    df = pd.read_csv(arquivo, low_memory=False)
    df["accessed_date"] = pd.to_datetime(df["accessed_date"])
    df["data"] = df["accessed_date"].dt.date  # Cria uma nova coluna "data" que contém apenas a parte da data (sem hora) da coluna "accessed_date"
    # Agrupar e acumular soma e contagem diretamente nos dicionários
    for group_name, group in df.groupby("data"):
        sum_sales[group_name] += group["sales"].sum()
        count[group_name] += len(group)

# Calcular total e média
vista = pd.DataFrame({
    "data": list(sum_sales.keys()),
    "total_vendas": list(sum_sales.values()),
    "media_vendas": [sum_sales[k] / count[k] for k in sum_sales]
})

pasta_saida = pasta_raiz / "vistas_lote"
os.makedirs(pasta_saida, exist_ok=True)
vista.to_csv(pasta_saida / "vendas_por_dia.csv", index=False)