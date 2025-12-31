ANP API / Dados Públicos
        ↓
Dataflow (Beam - Python)
        ↓
GCS (Bronze - Parquet)
        ↓
Spark (Dataproc / Databricks)
        ↓
GCS (Silver / Gold)
        ↓
BigQuery

##################

API ANP
  ↓
[ Beam / Dataflow ]
  ↓
🥉 Bronze (JSON Line/ raw)
  ↓
[ Spark / Beam SQL ]
  ↓
🥈 Silver (Parquet, schema)
  ↓
[ BQ / ML / BI ]
