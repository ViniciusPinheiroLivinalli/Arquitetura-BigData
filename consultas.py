import pandas as pd
from pathlib import Path

pasta_raiz = Path(r"C:\Users\vinil\Documents\GitHub\BigData")
pasta_lote = pasta_raiz / "vistas_lote"
pasta_vtr = pasta_raiz / "vistas_tempo_real"

# CARREGAR VISTAS DE LOTE
lote_vendas_dia = pd.read_csv(pasta_lote / "vendas_por_dia.csv")
lote_acessos_pais = pd.read_csv(pasta_lote / "acessos_por_pais.csv")
lote_vendas_metodo = pd.read_csv(pasta_lote / "vendas_por_metodo_pagamento.csv")
lote_duracao_browser = pd.read_csv(pasta_lote / "duracao_media_por_browser.csv")

# CARREGAR VISTAS DE TEMPO REAL
try:
    vtr_vendas_dia = pd.read_csv(pasta_vtr / "vendas_por_dia.csv")
    vtr_acessos_pais = pd.read_csv(pasta_vtr / "acessos_por_pais.csv")
    vtr_vendas_metodo = pd.read_csv(pasta_vtr / "vendas_por_metodo.csv")
    vtr_duracao_browser = pd.read_csv(pasta_vtr / "duracao_media_por_browser.csv")
except Exception as e:
    print(f"Erro ao carregar vistas de tempo real: {e}")
    print("Certifique-se de que o monitorar_fluxo.py está rodando antes de executar as consultas.")
    exit()

# CONSULTA 1 — VENDAS POR DIA
vtr_vendas_dia = vtr_vendas_dia.rename(columns={"date": "data"})

combinado_vendas_dia = (
    pd.concat([
        lote_vendas_dia[["data", "total_vendas"]],
        vtr_vendas_dia[["data", "total_vendas"]]
    ], ignore_index=True)
    .groupby("data", as_index=False)["total_vendas"]
    .sum()
    .sort_values("data")
    .reset_index(drop=True)
)
print("=== Vendas por Dia (completo) ===")
print(combinado_vendas_dia.to_string(index=False))
print()

# CONSULTA 2 — ACESSOS POR PAÍS
combinado_acessos_pais = (
    pd.concat([lote_acessos_pais, vtr_acessos_pais], ignore_index=True)
    .groupby("country", as_index=False)["total_acessos"]
    .sum()
    .sort_values("total_acessos", ascending=False)
    .reset_index(drop=True)
)
print("=== Acessos por País (completo) ===")
print(combinado_acessos_pais.to_string(index=False))
print()

# CONSULTA 3 — VENDAS POR MÉTODO DE PAGAMENTO
vtr_vendas_metodo = vtr_vendas_metodo.rename(columns={"method": "pay_method"})

combinado_vendas_metodo = (
    pd.concat([lote_vendas_metodo, vtr_vendas_metodo], ignore_index=True)
    .groupby("pay_method", as_index=False)
    .agg(total_vendas=("total_vendas", "sum"), contagem=("contagem", "sum"))
    .sort_values("total_vendas", ascending=False)
    .reset_index(drop=True)
)
print("=== Vendas por Método de Pagamento (completo) ===")
print(combinado_vendas_metodo.to_string(index=False))
print()

# CONSULTA 4 — DURAÇÃO MÉDIA POR BROWSER
vtr_duracao_browser = vtr_duracao_browser.rename(columns={"browser": "accessed_Ffom"})

combinado_duracao = pd.merge(
    lote_duracao_browser,
    vtr_duracao_browser,
    on="accessed_Ffom",
    how="outer",
    suffixes=("_lote", "_vtr")
)

def combinar_media(row):
    lote = row.get("media_duracao_lote")
    vtr = row.get("duracao_media")
    if pd.isna(lote):
        return vtr
    if pd.isna(vtr):
        return lote
    return (lote + vtr) / 2

combinado_duracao["media_duracao_combinada"] = combinado_duracao.apply(combinar_media, axis=1)

# remove linhas com NaN no resultado
combinado_duracao = (
    combinado_duracao[["accessed_Ffom", "media_duracao_combinada"]]
    .dropna()
    .sort_values("media_duracao_combinada", ascending=False)
    .reset_index(drop=True)
)

print("=== Duração Média por Browser (completo) ===")
print(combinado_duracao.to_string(index=False))
print()