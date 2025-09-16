# Databricks Delta Live Tables
import dlt
from pyspark.sql import functions as F, types as T

BRONZE_BASE   = spark.conf.get("gcfi.bronze_base")   # gs://gc-financial-insights-dev-bucket/bronze
CATALOG       = spark.conf.get("gcfi.uc_catalog", "main")
SCHEMA_BRONZE = spark.conf.get("gcfi.uc_schema_bronze", "bronze")
SCHEMA_SILVER = spark.conf.get("gcfi.uc_schema_silver", "silver")
EXPECT_PATH   = spark.conf.get("gcfi.expectations_path")


# ============== Helpers ==============
def read_expectations(path: str) -> dict:
    try:
        return spark.read.text(path).collect()[0][0]  # se estiver em DBFS
    except Exception:
        pass
    import json
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def base_schema():
    return T.StructType([
        T.StructField("date",   T.DateType()),
        T.StructField("ticker", T.StringType()),
        T.StructField("open",   T.DoubleType()),
        T.StructField("high",   T.DoubleType()),
        T.StructField("low",    T.DoubleType()),
        T.StructField("close",  T.DoubleType()),
        T.StructField("volume", T.DoubleType()),
    ])

EXPECTS = read_expectations(EXPECT_PATH) if EXPECT_PATH else {}

# ============== BRONZE (external view) ==============
# Auto Loader lida com novos arquivos; para histórico também funciona
@dlt.view(
    name=f"{CATALOG}.{SCHEMA_BRONZE}.prices_bronze_vw",
    comment="Raw prices parquet read from GCS"
)
def prices_bronze_vw():
    path = f"{BRONZE_BASE}/prices/**/*.parquet"
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "parquet")
          .load(path))
    # normaliza colunas (caso venham em maiúsculas)
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    # padroniza tipos
    df = (df
          .withColumn("date",   F.to_date("date"))
          .withColumn("ticker", F.upper(F.col("ticker")))
          .withColumn("open",   F.col("open").cast("double"))
          .withColumn("high",   F.col("high").cast("double"))
          .withColumn("low",    F.col("low").cast("double"))
          .withColumn("close",  F.col("close").cast("double"))
          .withColumn("volume", F.col("volume").cast("double"))
         )
    return df

# ============== SILVER ==============
@dlt.table(
    name=f"{CATALOG}.{SCHEMA_SILVER}.prices_silver",
    comment="Cleaned OHLCV with basic quality filters",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_close", "close IS NOT NULL AND close >= 0")
@dlt.expect_or_drop("valid_volume", "volume IS NULL OR volume >= 0")
@dlt.expect_or_drop("valid_range", "high >= low AND high >= open AND high >= close AND low <= open AND low <= close")
def prices_silver():
    df = dlt.read_stream(f"{CATALOG}.{SCHEMA_BRONZE}.prices_bronze_vw")
    df = df.dropDuplicates(["ticker","date"])
    return df

@dlt.table(
    name=f"{CATALOG}.{SCHEMA_SILVER}.prices_quarantine",
    comment="Records dropped by expectations"
)
def prices_quarantine():
    return dlt.read_stream("LIVE.prices_silver_expectations")  # metadados de expectations
