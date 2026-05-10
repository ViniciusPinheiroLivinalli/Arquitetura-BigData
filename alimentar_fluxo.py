import time
from pathlib import Path

pasta_raiz = Path(r"C:\Users\vinil\Documents\GitHub\BigData")

fonte = pasta_raiz / "dados_novos" / "fonte.csv"
fluxo = pasta_raiz / "dados_novos" / "fluxo.log"

with open(fonte, "r") as entrada:
    next(entrada)  # pula o cabeçalho
    with open(fluxo, "a") as saida:
        for linha in entrada:
            saida.write(linha)
            saida.flush()
            time.sleep(0.5)