import pandas as pd
from pathlib import Path
import os
from collections import defaultdict

pasta_raiz = Path(__file__).parent.parent  # Caminho para a raiz do projeto
pasta_dados = pasta_raiz / "dados_brutos"

# Usar dicionários para acumular soma e contagem por método de pagamento
sum_sales = defaultdict(float)
count = defaultdict(int)

for arquivo in pasta_dados.rglob("dados.csv"):  # rglob busca recursivamente
    df = pd.read_csv(arquivo, low_memory=False)
    df["accessed_date"] = pd.to_datetime(df["accessed_date"])
    # Agrupar e acumular soma e contagem diretamente nos dicionários
    for group_name, group in df.groupby("pay_method"):
        sum_sales[group_name] += group["sales"].sum()
        count[group_name] += len(group)

# Criar DataFrame final a partir dos dicionários
vista = pd.DataFrame({
    "pay_method": list(sum_sales.keys()),
    "soma_vendas": list(sum_sales.values()),
    "contagem_vendas": [count[k] for k in sum_sales]
})

pasta_saida = pasta_raiz / "vistas_lote"
os.makedirs(pasta_saida, exist_ok=True)
vista.to_csv(pasta_saida / "vendas_por_metodo_pagamento.csv", index=False)

# Log de processamento
pasta_gerenciamento = pasta_raiz / "gerenciamento"
os.makedirs(pasta_gerenciamento, exist_ok=True)
log_path = pasta_gerenciamento / "log_processamento.csv"
nova_entrada = pd.DataFrame([{
    "arquivo": str(pasta_saida / "vendas_por_metodo_pagamento.csv"),
    "timestamp": pd.Timestamp.now(),
    "qnt_linhas": len(vista),
    "status": "Processado"
}])

# Se o log já existe, adiciona ao final. Se não, cria.
if log_path.exists():
    nova_entrada.to_csv(log_path, mode="a", header=False, index=False)
else:
    nova_entrada.to_csv(log_path, index=False)