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

### 6. Simulação de Browser em Requisições de Worker
APIs governamentais (como a da ANP) frequentemente possuem firewalls ou filtros de segurança que bloqueiam User-Agents padrão de bibliotecas como requests ou urllib.

Problema: O código funcionava no Swagger/Local mas retornava vazio (0 registros) no Dataflow.

Solução: É obrigatório mimetizar os headers de um navegador real (User-Agent, Accept, Origin) dentro da DoFn. Sem o header Accept: application/json, a API pode simplesmente ignorar a requisição.

### 7. Paginação Baseada em Metadados (Pre-flight Request)
Evitar o "brute-force" de disparar milhares de requisições às cegas melhora a saúde do pipeline e reduz custos.

Técnica: Realizar uma requisição síncrona inicial (fora do pipeline) para capturar metadados de paginação.

Aprendizado: Identificamos que a ANP encapsula os controles de página no objeto searchPageFilter. Campos como totalPagina e totalRegistro devem ser usados para definir o range dinâmico do beam.Create.

### 8. Serialização e Escopo de Importação (Lazy Imports)
No Apache Beam, o código dentro de um DoFn é serializado e enviado para workers remotos.

Boas Práticas: Importar bibliotecas pesadas (como requests) dentro do método process.

Por que: Isso evita erros de NameError ou falhas de serialização caso o ambiente do worker tenha variações mínimas de instalação, garantindo que a dependência seja resolvida no momento da execução da tarefa.