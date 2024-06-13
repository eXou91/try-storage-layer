from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
minio_end_point = "http://minio-api.punch:8080"
minio_bucket = "iceberg"

iceberg_builder = SparkSession.builder \
    .appName("iceberg-spark-minio-example") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-hive-runtime:1.5.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", minio_end_point) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4")\
    .enableHiveSupport()
spark = configure_spark_with_delta_pip(iceberg_builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4","org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0" ]).getOrCreate()

# Read CSV file
csv_path = '/home/dorian/punchplatform/duckdb/deltalake/election.csv'  # Update this path to the actual location of your CSV file
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(csv_path)
file_count = df.count()

iceberg_table_location = f"s3a://{minio_bucket}"
# Write to Delta Lake
df.select(
    col("id_election"),
    col("id_brut_miom"),
    col("Code_du_departement"),
    col("Libelle_du_departement"),
    col("Code_de_la_commune"),
    col("Libelle_de_la_commune"),
    col("Inscrits"),
    col("Abstentions"),
    col("%_Abs/Ins"),
    col("Votants"),
    col("%_Vot/Ins"),
    col("Blancs"),
    col("%_Blancs/Ins"),
    col("%_Blancs/Vot"),
    col("Nuls"),
    col("%_Nuls/Ins"),
    col("%_Nuls/Vot"),
    col("Exprimes"),
    col("%_Exp/Ins"),
    col("%_Exp/Vot"),
    col("Code_de_la_circonscription"),
    col("Libelle_de_la_circonscription"),
    col("Code_du_canton"),
    col("Libelle_du_canton")
).write \
.format("iceberg") \
.mode("append") \
.saveAsTable("iceberg_election")  # Name of the Iceberg table


iceberg_df = spark.read.format("iceberg").load(f"{iceberg_table_location}/default/iceberg_election")
iceberg_df.printSchema()
iceberg_df.show()