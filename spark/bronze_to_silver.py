from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

def main():
    spark = SparkSession.builder \
        .appName("ANP_Bronze_to_Silver_Transformation") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    # 1. Definição do Schema para o Array de Produtos
    # Note que tancagem no seu JSON é decimal (7.5), então usamos Double ou Decimal
    produto_detalhe_schema = StructType([
        StructField("produto", StringType(), True),
        StructField("tancagem", DoubleType(), True),
        StructField("unidMedidaTancagem", StringType(), True),
        StructField("qtdeBicos", IntegerType(), True)
    ])

    # 2. Definição do Schema Bronze Principal
    # Corrigido: Adicionadas vírgulas e a estrutura ArrayType
    bronze_schema = StructType([
        StructField("codigoSIMP", StringType(), True),
        StructField("autorizacao", StringType(), True),
        StructField("dataPublicacao", StringType(), True),
        StructField("razaoSocial", StringType(), True),
        StructField("cnpj", StringType(), True),
        StructField("endereco", StringType(), True),
        StructField("complemento", StringType(), True),
        StructField("bairro", StringType(), True),
        StructField("cep", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("distribuidora", StringType(), True),
        StructField("dataVinculacao", StringType(), True),
        StructField("classe", StringType(), True),
        StructField("produtos", ArrayType(produto_detalhe_schema), True), # Estrutura aninhada
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("latitude_ANP4C", StringType(), True),
        StructField("longitude_ANP4C", StringType(), True),
        StructField("validacao", StringType(), True),
        StructField("estimativaAcuracia", StringType(), True),
        StructField("srid", StringType(), True),
        StructField("src", StringType(), True),
        StructField("dataObtencao", StringType(), True),
        StructField("origemInformacao", StringType(), True),
        StructField("situacaoConstatada", StringType(), True),
        StructField("observacao", StringType(), True),
        StructField("statusSIGAF", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True)
    ])

    # 3. Leitura (O JSON da ANP costuma ter os dados dentro de uma chave "data")
    path_bronze = "gs://bk_anp_raw/bronze/anp/combustivel/ingestion_date=2025-12-31/*.json"
    
    # Lemos o JSON e usamos a função inline para explodir a coluna 'data' se necessário
    # Se o seu arquivo for um array direto de objetos, use apenas spark.read.json
    df_raw = spark.read.option("multiline", "true").json(path_bronze, schema=bronze_schema)

# 4. Transformações e Explode (Camada Silver Completa)
    # Criamos uma linha para cada produto dentro da lista 'produtos'
    df_exploded = df_raw.withColumn("prod_item", F.explode(F.col("produtos")))

    df_silver = df_exploded.select(
        # Identificação e Localização
        F.col("codigoSIMP").alias("codigo_simp"),
        F.col("autorizacao").alias("autorizacao_anp"),
        F.to_date(F.col("dataPublicacao"), "dd/MM/yyyy").alias("data_publicacao"),
        F.upper(F.trim(F.col("razaoSocial"))).alias("razao_social"),
        F.col("cnpj"),
        F.upper(F.trim(F.col("endereco"))).alias("endereco"),
        F.upper(F.trim(F.col("complemento"))).alias("complemento"),
        F.upper(F.trim(F.col("bairro"))).alias("bairro"),
        F.col("cep"),
        F.upper(F.trim(F.col("uf"))).alias("uf"),
        F.upper(F.trim(F.col("municipio"))).alias("municipio"),
        F.upper(F.trim(F.col("distribuidora"))).alias("distribuidora"),
        F.to_date(F.col("dataVinculacao"), "dd/MM/yyyy").alias("data_vinculacao"),
        F.upper(F.trim(F.col("classe"))).alias("classe_posto"),

        # Dados do Produto (Vindos do Array Explodido)
        F.upper(F.trim(F.col("prod_item.produto"))).alias("produto_nome"),
        F.col("prod_item.tancagem").alias("tancagem_volume"),
        F.upper(F.trim(F.col("prod_item.unidMedidaTancagem"))).alias("unidade_medida_tancagem"),
        F.col("prod_item.qtdeBicos").alias("quantidade_bicos"),

        # Geolocalização e Precisão
        F.regexp_replace(F.col("latitude"), ",", ".").cast(DecimalType(10, 6)).alias("latitude"),
        F.regexp_replace(F.col("longitude"), ",", ".").cast(DecimalType(10, 6)).alias("longitude"),
        F.col("latitude_ANP4C").alias("latitude_anp4c"),
        F.col("longitude_ANP4C").alias("longitude_anp4c"),
        F.upper(F.trim(F.col("validacao"))).alias("status_validacao_geo"),
        F.col("estimativaAcuracia").cast(IntegerType()).alias("estimativa_acuracia_metros"),
        F.col("srid"),
        F.col("src").alias("sistema_referencia_geo"),

        # Metadados da ANP e Situação
        F.to_date(F.col("dataObtencao"), "dd/MM/yyyy").alias("data_obtencao_anp"),
        F.upper(F.trim(F.col("origemInformacao"))).alias("origem_informacao"),
        F.col("situacaoConstatada").alias("codigo_situacao_constatada"),
        F.col("observacao"),
        F.col("statusSIGAF").alias("status_sigaf"),

        # Auditoria de Processamento (Data Lakehouse)
        F.to_timestamp(F.col("ingestion_timestamp")).alias("ingestion_at"),
        F.current_timestamp().alias("processed_at")
    )

    # 5. Deduplicação (PK: CNPJ + Produto + Data de Obtenção)
    df_final = df_silver.dropDuplicates(["cnpj", "produto_nome", "data_obtencao_anp"])
    
    # 6. Escrita em Parquet
    path_silver = "gs://bk_anp_raw/silver/anp/combustivel/"
    (df_final.write
        .mode("overwrite")
        .partitionBy("uf", "data_obtencao_anp")
        .parquet(path_silver))

    print(f"Sucesso! Dados salvos em: {path_silver}")

if __name__ == "__main__":
    main()