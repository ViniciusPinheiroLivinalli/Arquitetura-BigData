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
vista_final = pd.DataFrame(list(totais_por_pais.items()), columns=["country", "total_acessos"])

pasta_saida = pasta_raiz / "vistas_lote"
os.makedirs(pasta_saida, exist_ok=True)
vista_final.to_csv(pasta_saida / "acessos_por_pais.csv", index=False)