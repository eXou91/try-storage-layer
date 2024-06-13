from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
minio_end_point = "http://minio-api.punch:8080"
minio_bucket = "deltalake"

# Start Spark session with Delta Lake extensions
builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", minio_end_point) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4")

spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]).getOrCreate()

# Read CSV file
csv_path = '/home/dorian/punchplatform/duckdb/deltalake/election.csv'  # Update this path to the actual location of your CSV file
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(csv_path)
file_count = df.count()

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
 .format("delta") \
 .partitionBy("Code_du_departement") \
 .mode("overwrite") \
 .save(f"s3a://{minio_bucket}/election-delta")

# Read from Delta Lake
delta_read_df = spark.read.format("delta").load(f"s3a://{minio_bucket}/election-delta")
uploaded_count = delta_read_df.count()

if file_count == uploaded_count:
    print("All written correctly!")
else:
    print("Some errors occurred.")
