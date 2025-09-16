import dlt
from pyspark.sql import functions as F

CATALOG       = spark.conf.get("gcfi.uc_catalog", "main")
SCHEMA_SILVER = spark.conf.get("gcfi.uc_schema_silver", "silver")
SCHEMA_GOLD   = spark.conf.get("gcfi.uc_schema_gold", "gold")

@dlt.table(
    name=f"{CATALOG}.{SCHEMA_GOLD}.prices_features",
    comment="Daily returns, moving averages and basic features (per ticker)",
    table_properties={"quality": "gold"}
)
def prices_features():
    s = dlt.read(f"{CATALOG}.{SCHEMA_SILVER}.prices_silver")
    w = (Window.partitionBy("ticker").orderBy(F.col("date").cast("timestamp"))
         .rowsBetween(-1, -1))
    from pyspark.sql.window import Window
    s = s.withColumn("ret", (F.col("close")/F.lag("close").over(Window.partitionBy("ticker").orderBy("date")) - 1.0))
    s = (s
         .withColumn("ma_20", F.avg("close").over(Window.partitionBy("ticker").orderBy("date").rowsBetween(-19,0)))
         .withColumn("ma_50", F.avg("close").over(Window.partitionBy("ticker").orderBy("date").rowsBetween(-49,0)))
         .withColumn("vol_20", F.stddev("ret").over(Window.partitionBy("ticker").orderBy("date").rowsBetween(-19,0)))
        )
    return s

# (opcional) se tiver earnings_silver, crie um join de evento ±3 dias
@dlt.view(name=f"{CATALOG}.{SCHEMA_GOLD}.earnings_effect_vw")
def earnings_effect_vw():
    try:
        p = dlt.read(f"{CATALOG}.{SCHEMA_SILVER}.prices_silver").select("date","ticker","close")
        e = dlt.read(f"{CATALOG}.{SCHEMA_SILVER}.earnings_silver").select("date","ticker").withColumnRenamed("date","earn_date")
    except Exception:
        return spark.createDataFrame([], "earn_date date, date date, ticker string, abnormal_return double")
    j = (p.join(e, "ticker", "inner")
           .where(F.abs(F.datediff("date","earn_date")) <= 3))
    w = Window.partitionBy("ticker","earn_date").orderBy("date")
    j = j.withColumn("ret", F.log(F.col("close")/F.lag("close").over(w)))
    # simplificado: abnormal = ret - média(ticker, 60d) no período anterior
    base = (p.withColumn("ret", F.log(F.col("close")/F.lag("close").over(Window.partitionBy("ticker").orderBy("date"))))
              .withColumn("avg60", F.avg("ret").over(Window.partitionBy("ticker").orderBy("date").rowsBetween(-60,-1))))
    j = (j.join(base.select("date","ticker","avg60"), ["date","ticker"], "left")
           .withColumn("abnormal_return", F.col("ret") - F.col("avg60")))
    return j.select("earn_date","date","ticker","abnormal_return")
