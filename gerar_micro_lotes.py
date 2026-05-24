import pandas as pd
import os
import time
from pathlib import Path

pasta_raiz = Path(r"C:\Users\vinil\Documents\GitHub\BigData")
fonte = pasta_raiz / "dados_novos" / "fonte.csv"
pasta_micro_lotes = pasta_raiz / "dados_novos" / "micro_lotes"
os.makedirs(pasta_micro_lotes, exist_ok=True)

# Tamanho do micro-lote: 500 linhas
# Critério: ~2.5% do tamanho de uma partição diária (~20.000 linhas)
# Consideravelmente menor, mas com volume suficiente para agregar com sentido
TAMANHO_LOTE = 500
INTERVALO = 5  # segundos entre cada micro-lote

df = pd.read_csv(fonte)
total_lotes = (len(df) // TAMANHO_LOTE) + 1

print(f"Total de registros: {len(df)}")
print(f"Tamanho do micro-lote: {TAMANHO_LOTE} linhas")
print(f"Total de micro-lotes a gerar: {total_lotes}")

for i, inicio in enumerate(range(0, len(df), TAMANHO_LOTE)):
    lote = df.iloc[inicio:inicio + TAMANHO_LOTE]
    caminho = pasta_micro_lotes / f"lote_{i:04d}.csv"
    lote.to_csv(caminho, index=False)
    print(f"Gerado: {caminho.name} ({len(lote)} linhas)")
    time.sleep(INTERVALO)

print("Todos os micro-lotes gerados!")