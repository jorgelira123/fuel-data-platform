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

> Docker é tratado como **parte do código da plataforma**, não apenas tooling local.

---

### 2. Diferença entre DirectRunner e DataflowRunner
- **DirectRunner**: execução local, utiliza credenciais do usuário
- **DataflowRunner**: execução distribuída, utiliza **Service Accounts**

Problemas de IAM e staging não aparecem no DirectRunner, mas surgem no DataflowRunner.

---

### 3. Staging e Temp não devem usar bucket raiz
Boa prática aplicada:
- nunca utilizar `gs://bucket/` diretamente
- sempre usar subdiretórios explícitos

Exemplo:
```
gs://bk_anp_raw-dataflow-staging/staging
gs://bk_anp_raw-dataflow-temp/temp
```
Isso evita falhas na criação e leitura do `pipeline.pb`.

---

### 4. IAM: Quem executa ≠ quem criou o bucket
Mesmo que o bucket exista e o usuário tenha acesso:
- o **Dataflow Service Account** precisa de permissões explícitas
- staging, temp e output exigem acesso independente

Service Account gerenciada pelo Dataflow:
`service-<PROJECT_NUMBER>@dataflow-service-producer-prod.iam.gserviceaccount.com`


> Em GCP, serviços **não usam credenciais humanas**.

---

### 5. Uso explícito de Service Account no Dataflow
A identidade de execução do pipeline é definida explicitamente:

```python
service_account = "dataflow-runner@<PROJECT_NUMBER>.iam.gserviceaccount.com"
