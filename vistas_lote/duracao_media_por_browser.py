import pandas as pd
from pathlib import Path
import os
from collections import defaultdict

pasta_raiz = Path(__file__).parent.parent  # Caminho para a raiz do projeto
pasta_dados = pasta_raiz / "dados_brutos"

# Usar dicionários para acumular soma e contagem por browser
sum_duracao = defaultdict(float)
count = defaultdict(int)

for arquivo in pasta_dados.rglob("dados.csv"):  # rglob busca recursivamente
    df = pd.read_csv(arquivo, low_memory=False)
    df["accessed_date"] = pd.to_datetime(df["accessed_date"])
    # Agrupar e acumular soma e contagem diretamente nos dicionários
    for group_name, group in df.groupby("accessed_Ffom"):
        sum_duracao[group_name] += group["duration_(secs)"].sum()
        count[group_name] += len(group)

# Calcular média
media_duracao = {k: sum_duracao[k] / count[k] for k in sum_duracao}

# Criar DataFrame final a partir do dicionário
vista = pd.DataFrame(list(media_duracao.items()), columns=["accessed_Ffom", "media_duracao"])

pasta_saida = pasta_raiz / "vistas_lote"
os.makedirs(pasta_saida, exist_ok=True)
vista.to_csv(pasta_saida / "duracao_media_por_browser.csv", index=False)

# Log de processamento
pasta_gerenciamento = pasta_raiz / "gerenciamento"
os.makedirs(pasta_gerenciamento, exist_ok=True)
log_path = pasta_gerenciamento / "log_processamento.csv"
nova_entrada = pd.DataFrame([{
    "arquivo": str(pasta_saida / "duracao_media_por_browser.csv"),
    "timestamp": pd.Timestamp.now(),
    "qnt_linhas": len(vista),
    "status": "Processado"
}])

# Se o log já existe, adiciona ao final. Se não, cria.
if log_path.exists():
    nova_entrada.to_csv(log_path, mode="a", header=False, index=False)
else:
    nova_entrada.to_csv(log_path, index=False)