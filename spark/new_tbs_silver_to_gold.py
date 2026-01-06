from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("ANP_Silver_to_Gold_Analytics")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=== Iniciando job Silver -> Gold ===")

    # ===============================
    # 1. Leitura da camada Silver
    # ===============================
    path_silver = "gs://bk_anp_raw/silver/anp/combustivel/"
    df_silver = spark.read.parquet(path_silver)

    if df_silver.rdd.isEmpty():
        raise RuntimeError("Silver vazia ou caminho inválido")

    df_silver = df_silver.cache()
    print("Silver carregada com sucesso")

    # ===============================
    # Helper: Conversão ANP4C (GMS) -> Decimal
    # ===============================
    def convert_anp4c_to_decimal(col_name):
        sign = F.when(F.substring(F.col(col_name), 1, 1) == "-", -1.0).otherwise(1.0)
        clean_col = F.regexp_replace(
            F.regexp_replace(F.col(col_name), "-", ""),
            ",",
            "."
        )
        parts = F.split(clean_col, ":")
        return sign * (
            parts.getItem(0).cast("double") +
            parts.getItem(1).cast("double") / 60.0 +
            parts.getItem(2).cast("double") / 3600.0
        )

    # ===============================
    # Enriquecimento geográfico
    # ===============================
    df_with_geo = (
        df_silver
        .withColumn("latitude_mapa", convert_anp4c_to_decimal("latitude_anp4c"))
        .withColumn("longitude_mapa", convert_anp4c_to_decimal("longitude_anp4c"))
        .cache()
    )

    print("Geo enriquecido")

    # ===============================
    # 1. TB_GOLD_POSTO
    # ===============================
    df_gold_posto = df_with_geo.groupBy("cnpj").agg(
        F.max("razao_social").alias("razao_social"),
        F.max("uf").alias("uf"),
        F.max("municipio").alias("municipio"),
        F.max("bairro").alias("bairro"),
        F.max("latitude_mapa").alias("latitude_mapa"),
        F.max("longitude_mapa").alias("longitude_mapa"),
        F.max("classe_posto").alias("classe_posto"),
        F.max("distribuidora").alias("distribuidora_principal"),
        F.sum("tancagem_volume").alias("tancagem_total_posto"),
        F.sum("quantidade_bicos").alias("quantidade_bicos_total"),
        F.concat_ws(", ", F.collect_set("produto_nome")).alias("produtos_disponiveis"),
        F.max("data_obtencao_anp").alias("ultima_atualizacao")
    )

    # ===============================
    # 2. TB_GOLD_CAPACIDADE_PRODUTO
    # ===============================
    df_gold_capacidade_produto = df_silver.groupBy(
        "uf", "municipio", "produto_nome"
    ).agg(
        F.countDistinct("cnpj").alias("qtd_postos"),
        F.sum("tancagem_volume").alias("capacidade_estatica_total"),
        F.when(
            F.countDistinct("cnpj") > 0,
            F.sum("tancagem_volume") / F.countDistinct("cnpj")
        ).otherwise(0).alias("capacidade_media_por_posto")
    )

    # ===============================
    # 3. TB_GOLD_DISTRIBUIDORA
    # ===============================
    df_gold_distribuidora = df_silver.groupBy(
        "distribuidora", "uf", "municipio"
    ).agg(
        F.countDistinct("cnpj").alias("qtd_postos"),
        F.sum("tancagem_volume").alias("capacidade_total"),
        F.when(
            F.countDistinct("cnpj") > 0,
            F.sum("quantidade_bicos") / F.countDistinct("cnpj")
        ).otherwise(0).alias("media_bicos_por_posto"),
        F.concat_ws(", ", F.collect_set("produto_nome")).alias("mix_produtos")
    )

    # ===============================
    # 4. TB_GOLD_GEO_QUALIDADE
    # ===============================
    df_gold_geo_qualidade = df_with_geo.groupBy(
        "uf", "municipio"
    ).agg(
        F.countDistinct("cnpj").alias("total_postos"),
        F.countDistinct(
            F.when(
                F.col("latitude_mapa").isNotNull() &
                F.col("longitude_mapa").isNotNull(),
                F.col("cnpj")
            )
        ).alias("postos_geo_validos"),
        F.avg("estimativa_acuracia_metros").alias("acuracia_media_metros")
    ).withColumn(
        "percentual_geo_valido",
        F.when(
            F.col("total_postos") > 0,
            (F.col("postos_geo_validos") / F.col("total_postos")) * 100
        ).otherwise(0)
    )

    # ===============================
    # 5. TB_GOLD_TEMPORAL
    # ===============================
    df_gold_temporal = df_silver.groupBy(
        "data_obtencao_anp"
    ).agg(
        F.countDistinct("cnpj").alias("qtd_postos"),
        F.sum("tancagem_volume").alias("capacidade_total"),
        F.countDistinct(
            F.when(
                F.col("data_vinculacao") == F.col("data_obtencao_anp"),
                F.col("cnpj")
            )
        ).alias("novos_postos")
    )

    # ===============================
    # Escrita da camada Gold
    # ===============================
    path_gold = "gs://bk_anp_raw/gold/anp/"

    df_gold_posto.repartition("uf").write.mode("overwrite") \
        .partitionBy("uf") \
        .parquet(path_gold + "tb_gold_posto")

    df_gold_capacidade_produto.repartition("uf").write.mode("overwrite") \
        .partitionBy("uf") \
        .parquet(path_gold + "tb_gold_capacidade_produto")

    df_gold_distribuidora.repartition("uf").write.mode("overwrite") \
        .partitionBy("uf") \
        .parquet(path_gold + "tb_gold_distribuidora")

    df_gold_geo_qualidade.write.mode("overwrite") \
        .parquet(path_gold + "tb_gold_geo_qualidade")

    df_gold_temporal.write.mode("overwrite") \
        .parquet(path_gold + "tb_gold_temporal")

    print("=== Job finalizado com sucesso ===")
    spark.stop()


if __name__ == "__main__":
    main()
