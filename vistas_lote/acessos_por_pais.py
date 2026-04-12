import pandas as pd
from pathlib import Path
import os
from collections import defaultdict

pasta_raiz = Path(__file__).parent.parent  # Caminho para a raiz do projeto
pasta_dados = pasta_raiz / "dados_brutos"

# Usar um dicionário para acumular totais por país
totais_por_pais = defaultdict(int)

for arquivo in pasta_dados.rglob("dados.csv"):  # rglob busca recursivamente
    df = pd.read_csv(arquivo, low_memory=False)
    # Agrupar e somar diretamente no dicionário
    for country, group in df.groupby("country"):
        totais_por_pais[country] += len(group)

# Criar DataFrame final a partir do dicionário
vista = pd.DataFrame(list(totais_por_pais.items()), columns=["country", "total_acessos"])

pasta_saida = pasta_raiz / "vistas_lote"
os.makedirs(pasta_saida, exist_ok=True)

pasta_gerenciamento = pasta_raiz / "gerenciamento"
os.makedirs(pasta_gerenciamento, exist_ok=True)

# LOG DE PROCESSAMENTO
log_path = pasta_gerenciamento / "log_processamento.csv"

nova_entrada = pd.DataFrame([{
    "arquivo": str(pasta_saida / "acessos_por_pais.csv"),
    "timestamp": pd.Timestamp.now(),
    "qnt_linhas": len(vista),
    "status": "Processado"
}])

# Se o log já existe, adiciona ao final. Se não, cria.
if log_path.exists():
    nova_entrada.to_csv(log_path, mode="a", header=False, index=False)
else:
    nova_entrada.to_csv(log_path, index=False)
