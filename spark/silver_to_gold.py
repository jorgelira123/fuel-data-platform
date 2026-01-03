from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("ANP_Silver_to_Gold_Analytics") \
        .getOrCreate()

    # 1. Leitura da Silver
    path_silver = "gs://bk_anp_raw/silver/anp/combustivel/"
    df_silver = spark.read.parquet(path_silver)

    # Função para converter GMS (ANP4C) para Decimal (Necessário para o Mapa)
    def convert_anp4c_to_decimal(col_name):
        sign = F.when(F.substring(F.col(col_name), 1, 1) == "-", -1.0).otherwise(1.0)
        clean_col = F.regexp_replace(F.regexp_replace(F.col(col_name), "-", ""), ",", ".")
        parts = F.split(clean_col, ":")
        return (sign * (
            parts.getItem(0).cast("double") + 
            parts.getItem(1).cast("double") / 60.0 + 
            parts.getItem(2).cast("double") / 3600.0
        ))

    # --- TABELA 1: GOLD GEOGRAFIA (Para o Mapa) ---
    # Correção: Agora usando VÍRGULAS para separar as colunas
    df_gold_geografia = df_silver.groupBy(
        "latitude_anp4c", 
        "longitude_anp4c", 
        "uf", 
        "municipio", 
        "distribuidora", 
        "cnpj", 
        "razao_social"
    ).agg(
        convert_anp4c_to_decimal("latitude_anp4c").alias("latitude_mapa"),
        convert_anp4c_to_decimal("longitude_anp4c").alias("longitude_mapa"),
        F.concat_ws(", ", F.collect_set("produto_nome")).alias("produtos_disponiveis"),
        F.sum("tancagem_volume").alias("tancagem_total_posto"),
        F.max("data_obtencao_anp").alias("ultima_atualizacao")
    )

    # --- TABELA 2: GOLD MERCADO (Estatísticas) ---
    df_gold_mercado = df_silver.groupBy(
        "uf", 
        "municipio", 
        "distribuidora", 
        "produto_nome"
    ).agg(
        F.countDistinct("cnpj").alias("qtd_postos"),
        F.sum("tancagem_volume").alias("capacidade_estatica_total"),
        F.avg("quantidade_bicos").alias("media_bicos_por_posto")
    )

    # 2. Escrita dos resultados na pasta Gold
    path_gold = "gs://bk_anp_raw/gold/anp/"
    
    df_gold_geografia.write.mode("overwrite").parquet(path_gold + "geografia_postos")
    df_gold_mercado.write.mode("overwrite").parquet(path_gold + "mercado_distribuidoras")

    print("=== Camada Gold gerada com sucesso! ===")
    spark.stop()

if __name__ == "__main__":
    main()