from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Configurações básicas
PROJECT_ID = "fuel-data-project-482021"
REGION = "us-east1"
CLUSTER_NAME = "cluster-gold-al"
PYSPARK_JOB_PATH = "gs://bk_anp_raw/gold/scripts/new_tbs_silver_to_gold.py"

# Configurações do Cluster
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_size_gb": 32},
    },
    "software_config": {"image_version": "2.1-debian11"},
    "lifecycle_config": {"idle_delete_ttl": {"seconds": 300}}, # 5min ocioso
}

default_args = {
    'owner': 'engenharia_dados',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fuel_pipeline_anp_gold',
    default_args=default_args,
    description='Pipeline de processamento ANP: Silver para Gold Alagoas',
    schedule_interval='@daily',
    catchup=False,
    tags=['ANP', 'ALAGOAS', 'GOLD'],
) as dag:

    # 1. CRIA O CLUSTER
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
    )

    # 1. Script Spark
    process_gold_spark = DataprocSubmitJobOperator(
        task_id='process_gold_spark',
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": PYSPARK_JOB_PATH},
        },
        region=REGION,
        project_id=PROJECT_ID,
        
    )

    # 2. Tarefa para Atualizar a View no BigQuery
    refresh_gold_views = BigQueryInsertJobOperator(
        task_id='refresh_gold_views_alagoas',
        configuration={
            "query": {
                "query": """
                -- 1. View de Postos (Mapa)
                CREATE OR REPLACE VIEW `fuel-data-project-482021.fuel_analysis.vw_looker_postos_al` AS
                SELECT * FROM `fuel-data-project-482021.fuel_analysis.tb_gold_posto`
                WHERE uf = 'AL';

                -- 2. View de Capacidade por Produto
                CREATE OR REPLACE VIEW `fuel-data-project-482021.fuel_analysis.vw_looker_capacidade_al` AS
                SELECT * FROM `fuel-data-project-482021.fuel_analysis.tb_gold_capacidade_produto`
                WHERE uf = 'AL';

                -- 3. View de Distribuidoras
                CREATE OR REPLACE VIEW `fuel-data-project-482021.fuel_analysis.vw_looker_distribuidoras_al` AS
                SELECT * FROM `fuel-data-project-482021.fuel_analysis.tb_gold_distribuidora`
                WHERE uf = 'AL';

                -- 4. View de Qualidade Geográfica
                CREATE OR REPLACE VIEW `fuel-data-project-482021.fuel_analysis.vw_looker_geo_qualidade_al` AS
                SELECT * FROM `fuel-data-project-482021.fuel_analysis.tb_gold_geo_qualidade`
                WHERE uf = 'AL';

                -- 5. View Temporal (Histórico)
                CREATE OR REPLACE VIEW `fuel-data-project-482021.fuel_analysis.vw_looker_temporal_al` AS
                SELECT * FROM `fuel-data-project-482021.fuel_analysis.tb_gold_temporal`;
                """,
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID
    )

    # 4. DELETA O CLUSTER
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE, # Morra mesmo se o spark falhar
    )

    # Fluxo de Automacao
    create_cluster >> process_gold_spark >> refresh_gold_views >> delete_cluster