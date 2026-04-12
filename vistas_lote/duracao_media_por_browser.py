import pandas as pd
from pathlib import Path
import os

pasta_raiz = Path(r"C:\Users\vinil\Documents\GitHub\BigData") # Altere para o caminho correto do seu projeto
pasta_dados = pasta_raiz / "dados_brutos"

dataframes = []

for arquivo in pasta_dados.rglob("dados.csv"):  # rglob busca recursivamente
    df = pd.read_csv(arquivo, low_memory=False)
    dataframes.append(df)

df_total = pd.concat(dataframes, ignore_index=True)
df_total["accessed_date"] = pd.to_datetime(df_total["accessed_date"])

##

vista = df_total.groupby("accessed_Ffom")["duration_(secs)"].agg(
    media_duracao="mean",
).reset_index() 

##

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