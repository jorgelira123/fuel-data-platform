# Fuel Data Platform – ANP

Plataforma de dados para ingestão, processamento e análise de preços de combustíveis no Brasil, utilizando serviços gerenciados do Google Cloud Platform.

## Objetivo
Centralizar dados públicos da ANP, garantindo:
- Ingestão confiável
- Processamento escalável
- Dados analíticos prontos para consumo
- Controle de custo e qualidade

## Arquitetura
- Ingestão: Dataflow (Apache Beam)
- Storage: Google Cloud Storage (Parquet)
- Processamento: Spark (Dataproc / Databricks)
- Orquestração: Cloud Composer (Airflow)
- Analytics: BigQuery

## Camadas de Dados
- **Bronze**: dados brutos da ANP
- **Silver**: dados limpos e normalizados
- **Gold**: métricas analíticas

## Status
🚧 Em desenvolvimento
