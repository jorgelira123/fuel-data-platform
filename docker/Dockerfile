FROM python:3.11-slim-bullseye

# Evita prompts interativos
ENV DEBIAN_FRONTEND=noninteractive

# Dependências de sistema + Java + utilitários
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-11-jre-headless \
        curl \
        git \
        ca-certificates \
        gnupg \
        apt-transport-https \
        lsb-release \
    && rm -rf /var/lib/apt/lists/*

# Adiciona repositório do Google Cloud SDK
RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    > /etc/apt/sources.list.d/google-cloud-sdk.list

# Instala gcloud CLI
RUN apt-get update \
    && apt-get install -y google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

# Variáveis Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV GOOGLE_CLOUD_QUOTA_PROJECT=fuel-data-project-482021

# Diretório de trabalho
WORKDIR /app

# Dependências Python
COPY docker/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Código do projeto
COPY . /app

# Container fica ativo para execuções manuais
CMD ["tail", "-f", "/dev/null"]