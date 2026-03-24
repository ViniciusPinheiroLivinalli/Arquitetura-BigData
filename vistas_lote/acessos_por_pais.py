import pandas as pd
from pathlib import Path
import os

pasta_raiz = Path(r"C:\Users\vinil\OneDrive\Documentos\GitHub\Arquitetura-BigData") # Altere para o caminho correto do seu projeto
pasta_dados = pasta_raiz / "dados_brutos"

dataframes = []

for arquivo in pasta_dados.rglob("dados.csv"):  # rglob busca recursivamente
    df = pd.read_csv(arquivo, low_memory=False)
    dataframes.append(df)

df_total = pd.concat(dataframes, ignore_index=True)
df_total["accessed_date"] = pd.to_datetime(df_total["accessed_date"])

##

vista = df_total.groupby("country")["accessed_date"].agg(
    total_acessos="count",
).reset_index() 

##

pasta_saida = pasta_raiz / "vistas_lote"
os.makedirs(pasta_saida, exist_ok=True)
vista.to_csv(pasta_saida / "acessos_por_pais.csv", index=False)