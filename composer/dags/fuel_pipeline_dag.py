from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

# Configurações básicas
PROJECT_ID = "fuel-data-project-482021"
REGION = "us-east1"
CLUSTER_NAME = "cluster-silver-rapido"
PYSPARK_JOB_PATH = "gs://bk_anp_raw/gold/scripts/new_tbs_silver_to_gold.py"

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
    description='Pipeline de processamento ANP: Silver para Gold',
    schedule_interval='@daily',
    catchup=False,
    tags=['ANP', 'ALAGOAS', 'GOLD'],
) as dag:

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

    # Fluxo de execução
    process_gold_spark >> refresh_bigquery_view