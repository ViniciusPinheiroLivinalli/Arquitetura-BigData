# Arquitetura Big Data — Processamento de Dados com Spark

Projeto de arquitetura de dados inspirado no modelo Lambda Architecture, combinando processamento em lote (batch) e em tempo real (streaming) utilizando Apache Spark. O projeto usa como base um conjunto de logs de um e-commerce, simulando um pipeline completo de ingestão, processamento e geração de vistas analíticas.

## Visão Geral

O objetivo do projeto é demonstrar, de ponta a ponta, como dados brutos gerados continuamente (logs de um site de e-commerce) podem ser processados tanto em lotes históricos quanto em fluxo contínuo, gerando vistas de dados prontas para consulta e análise.

## A arquitetura segue três camadas principais:

Camada de Lote (Batch Layer): processa o histórico completo de dados brutos, gerando vistas consolidadas e confiáveis.
Camada de Velocidade (Speed Layer): processa dados em tempo real conforme chegam, complementando as vistas em lote com informações mais recentes.
Camada de Serviço (Serving Layer): disponibiliza as vistas (lote + tempo real) para consultas.

## Tecnologias Utilizadas
Apache Spark (processamento distribuído — batch e streaming)
Python (scripts de processamento e consultas)
PowerShell (configuração de ambiente)
CSV como formato de dados brutos de entrada
