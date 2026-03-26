import pandas as pd
from pathlib import Path
import os

pasta_raiz = Path(r"C:\Users\vinil\Documents\GitHub\BigData") 
pasta_dados = pasta_raiz / "dados_brutos"

pasta_gerenciamento = pasta_raiz / "gerenciamento"
os.makedirs(pasta_gerenciamento, exist_ok=True)

log = []
quarentena_linhas = [] 
quarentena_por_arquivo = [] 

arquivos_encontrados = list(pasta_dados.rglob("dados.csv"))
print(f"Arquivos encontrados: {len(arquivos_encontrados)}")
for a in arquivos_encontrados:
    print(a)

for arquivo in pasta_dados.rglob("dados.csv"):  # rglob busca recursivamente
    df = pd.read_csv(arquivo, low_memory=False) # ler o csv
    linhas_iniciais = len(df)
    mask_sales = (df["sales"] < 0)
    mask_age = pd.to_numeric(df["age"], errors="coerce").isnull()
    mask_problemas = mask_age | mask_sales
    df_ok = df[~mask_problemas] # Linhas sem problemas
    df_ruim = df[mask_problemas] # Linhas com problemas
    df_ruim = df_ruim.copy()
    df_ruim["arquivo_origem"] = str(arquivo)
    log.append({
        "arquivo": str(arquivo),
        "timestamp": pd.Timestamp.now(),
        "qnt_linhas": linhas_iniciais,
        "status": "Processado"
    })
    quarentena_linhas.append(df_ruim)
    quarentena_por_arquivo.append({
        "arquivo": str(arquivo),
        "qnt_linhas_quarentena": len(df_ruim)
    })

##

pd.DataFrame(log).to_csv(pasta_gerenciamento / "log_processamento.csv", index=False)
if quarentena_linhas:
    pd.concat(quarentena_linhas, ignore_index=True).to_csv(
        pasta_gerenciamento / "quarentena_linhas_brutas.csv", index=False)
else:
    pd.DataFrame().to_csv(
        pasta_gerenciamento / "quarentena_linhas_brutas.csv", index=False)
    print("Nenhuma linha problemática encontrada.")
pd.DataFrame(quarentena_por_arquivo).to_csv(pasta_gerenciamento / "quarentena_linhas_por_arquivo.csv", index=False)