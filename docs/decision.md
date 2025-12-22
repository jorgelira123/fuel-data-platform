# Decisões Arquiteturais

## Por que Dataflow para ingestão?
- Escalabilidade automática
- Retry e paralelismo nativos
- Boa integração com GCS
- Ideal para ingestão batch e ETL simples

## Por que Spark para transformação?
- Melhor performance para agregações complexas
- Otimizações avançadas (shuffle, joins, partitions)
- Padrão de mercado para dados analíticos

## Por que Composer?
- Orquestração desacoplada
- Suporte a backfill
- Observabilidade e SLA
- Padrão enterprise

