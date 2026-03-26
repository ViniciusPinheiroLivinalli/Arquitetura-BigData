import pandas as pd
import os
from pathlib import Path

caminho_csv = Path("E-commerce Website Logs.csv")
df = pd.read_csv(caminho_csv, low_memory=False)

df["accessed_date"] = pd.to_datetime(df["accessed_date"])

df["ano"] = df["accessed_date"].dt.year
df["mes"] = df["accessed_date"].dt.month
df["dia"] = df["accessed_date"].dt.day

pasta_raiz = Path(r"C:\Users\vinil\Documents\GitHub\BigData")

for (ano, mes, dia), grupo in df.groupby(["ano", "mes", "dia"]):
    pasta_particao = pasta_raiz / "dados_brutos" / str(ano) / f"{mes:02d}" / f"{dia:02d}"
    os.makedirs(pasta_particao, exist_ok=True)
    grupo_limpo = grupo.drop(columns=["ano", "mes", "dia"])
    grupo_limpo.to_csv(pasta_particao / "dados.csv", index=False)
