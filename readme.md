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

---

## Lições Aprendidas (Engineering Notes)

### 1. Ambiente Reprodutível com Docker
Todo o ambiente de desenvolvimento é versionado via:
- `Dockerfile`
- `docker-compose.yml`
- `requirements.txt`

Benefícios:
- consistência entre dev / CI / produção  
- isolamento de dependências (Beam, Java, GCP SDK)  
- onboarding rápido

### 2. Diferença entre DirectRunner e DataflowRunner
- **DirectRunner**: execução local, utiliza credenciais do usuário
- **DataflowRunner**: execução distribuída, utiliza **Service Accounts**

Problemas de IAM e staging não aparecem no DirectRunner, mas surgem no DataflowRunner.

