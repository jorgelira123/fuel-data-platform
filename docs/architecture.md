# Arquitetura da Plataforma

## Visão Geral
A plataforma segue o padrão de arquitetura em camadas (Medallion Architecture), separando ingestão, processamento e consumo.

## Fluxo
1. Dados são extraídos da ANP via Dataflow
2. Persistidos no GCS (camada Bronze)
3. Processados via Spark para Silver e Gold
4. Disponibilizados no BigQuery para consumo

## Benefícios
- Escalabilidade
- Observabilidade
- Facilidade de reprocessamento
- Redução de acoplamento
