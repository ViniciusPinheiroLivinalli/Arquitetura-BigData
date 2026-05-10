import threading
import time
import csv
from pathlib import Path

pasta_raiz = Path(r"C:\Users\vinil\Documents\GitHub\BigData")
pasta_vtr = pasta_raiz / "vistas_tempo_real"
arquivo_fluxo = pasta_raiz / "dados_novos" / "fluxo.log"

finaliza_laco = False

# Acumuladores
vendas_por_dia = {}
acessos_por_pais = {}
vendas_por_metodo = {}
duracao_por_browser = {}

def processar_linha(linha):
    campos = linha.split(",")
    
    data = campos[0][:10]
    duracao = float(campos[1])
    browser = campos[5]
    pais = campos[8]
    sales = float(campos[11])
    metodo = campos[14]

    if pais in acessos_por_pais:
        acessos_por_pais[pais] += 1
    else:
        acessos_por_pais[pais] = 1  

    if data in vendas_por_dia:
        vendas_por_dia[data]["total"] += sales
        vendas_por_dia[data]["contagem"] += 1
    else:
        vendas_por_dia[data] = {"total": sales, "contagem": 1}

    # ✅ CORRIGIDO: vendas_por_metodo agora guarda total e contagem
    if metodo in vendas_por_metodo:
        vendas_por_metodo[metodo]["total"] += sales
        vendas_por_metodo[metodo]["contagem"] += 1
    else:
        vendas_por_metodo[metodo] = {"total": sales, "contagem": 1}

    if browser in duracao_por_browser:
        duracao_por_browser[browser]["soma"] += duracao
        duracao_por_browser[browser]["contagem"] += 1
    else:
        duracao_por_browser[browser] = {"soma": duracao, "contagem": 1}

def salvar_vistas():
    # Acessos por país
    with open(pasta_vtr / "acessos_por_pais.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["country", "total_acessos"])
        for pais, total in acessos_por_pais.items():
            writer.writerow([pais, total])

    # Vendas por dia
    with open(pasta_vtr / "vendas_por_dia.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "total_vendas", "contagem"])
        for data, dados in vendas_por_dia.items():
            writer.writerow([data, dados["total"], dados["contagem"]])

    # Vendas por método
    with open(pasta_vtr / "vendas_por_metodo.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["method", "total_vendas", "contagem"])
        for metodo, dados in vendas_por_metodo.items():
            writer.writerow([metodo, dados["total"], dados["contagem"]])

    # Duração média por browser
    with open(pasta_vtr / "duracao_media_por_browser.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["browser", "duracao_media"])
        for browser, dados in duracao_por_browser.items():
            duracao_media = dados["soma"] / dados["contagem"]
            writer.writerow([browser, duracao_media])

def stream_dados(arquivo):
    with open(arquivo, "r") as arq:
        while not finaliza_laco:
            linha = arq.readline().strip()
            if not linha:
                time.sleep(1)
                continue
            yield linha

def monitora_linhas(arquivo):
    print("inicializando monitoramento")
    for nova_linha in stream_dados(arquivo):
        print(f"processando nova linha:\n\t{nova_linha}")
        processar_linha(nova_linha)
        salvar_vistas()
    print("finalizando monitoramento")

if __name__ == "__main__":
    t = threading.Thread(target=monitora_linhas, args=(arquivo_fluxo,))
    t.start()

    input("Pressione ENTER para encerrar o monitoramento...\n")
    
    finaliza_laco = True
    time.sleep(1.5)
    print("Monitoramento encerrado.")