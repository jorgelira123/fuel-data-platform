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

### 3. Simulação de Browser em Requisições de Worker
APIs governamentais (como a da ANP) frequentemente possuem firewalls ou filtros de segurança que bloqueiam User-Agents padrão de bibliotecas como requests ou urllib.

Problema: O código funcionava no Swagger/Local mas retornava vazio (0 registros) no Dataflow.

Solução: É obrigatório mimetizar os headers de um navegador real (User-Agent, Accept, Origin) dentro da DoFn. Sem o header Accept: application/json, a API pode simplesmente ignorar a requisição.

### 4. Paginação Baseada em Metadados (Pre-flight Request)
Evitar o "brute-force" de disparar milhares de requisições às cegas melhora a saúde do pipeline e reduz custos.

Técnica: Realizar uma requisição síncrona inicial (fora do pipeline) para capturar metadados de paginação.

Aprendizado: Identificamos que a ANP encapsula os controles de página no objeto searchPageFilter. Campos como totalPagina e totalRegistro devem ser usados para definir o range dinâmico do beam.Create.

### 5. Serialização e Escopo de Importação (Lazy Imports)
No Apache Beam, o código dentro de um DoFn é serializado e enviado para workers remotos.

Boas Práticas: Importar bibliotecas pesadas (como requests) dentro do método process.

Por que: Isso evita erros de NameError ou falhas de serialização caso o ambiente do worker tenha variações mínimas de instalação, garantindo que a dependência seja resolvida no momento da execução da tarefa.