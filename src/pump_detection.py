from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr,mean,stddev ,percentile_approx,broadcast

def compute_thresholds(df: DataFrame):
    return df.groupBy("symbol").agg(
        (mean("returns") + 2 * stddev("returns")).alias("returns_threshold"),
        expr("percentile_approx(volume, 0.95)").alias("volume_threshold")
    )


def apply_pump_flags(df: DataFrame, thresholds) -> DataFrame:
    df = df.join(broadcast(thresholds), "symbol")
    return (
        df.withColumn("volume_spike", col("volume") > col("volume_threshold"))
          .withColumn("price_jump", col("returns") > col("returns_threshold"))
          .withColumn("taker_aggress", col("taker_ratio") > 0.7)
          .withColumn("pump_flag", col("volume_spike") & col("price_jump") & col("taker_aggress"))
    )

def get_top_pumps(df:DataFrame,top_n:int=10)->DataFrame:
    return (
          df.filter(col("pump_flag"))
            .withColumn("severity_score",col("returns")*col("taker_ratio")*(col("volume")/col("rolling_avg_volume")))
            .select("symbol","open_time","close","returns","taker_ratio","volume","rolling_avg_volume","severity_score")
            .orderBy(col("severity_score").desc())
            .limit(top_n)
            )
    