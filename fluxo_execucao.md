Preparação em cada terminal antes de rodar (Configurar variáveis de ambiente):

$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"

---

Limpeza antes de cada teste:

Remove-Item -Recurse -Force "dados_novos\checkpoint" -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force "dados_novos\micro_lotes\*" -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force "vistas_velocidade_spark\*" -ErrorAction SilentlyContinue

---

Passo 1 - Camada de Lote (Aguarda terminar. Gera vistas_lote_spark/.):

python spark_camada_lote.py

---

Passo 2 — Terminal 1 (inicia o streaming):

python spark_camada_velocidade.py

(Aguarda aparecer "Streaming iniciado! Aguardando micro-lotes...")

---

Passo 3 — Terminal 2 (alimenta os micro-lotes):

python gerar_micro_lotes.py

Aguarda gerar todos os 54 lotes.

---

Passo 4 — Terminal 3 (consultas combinadas):

python spark_camada_apresentacao.py

---

Passo 5 — Encerrar o streaming

No Terminal 1, pressione Ctrl+C.