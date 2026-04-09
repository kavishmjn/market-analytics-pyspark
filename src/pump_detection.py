from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def add_pump_flags(df: DataFrame)->DataFrame:
    return  (
             df.withColumn("volume_spike",col("volume")>2*(col("rolling_avg_volume")))
               .withColumn("price_jump",col("returns")>0.003)
               .withColumn("taker_aggress",col("taker_ratio")>0.7)
               .withColumn("pump_flag",col("volume_spike") & col("price_jump") & col("taker_aggress"))
            )

def get_top_pumps(df:DataFrame,top_n:int=10)->DataFrame:
    return (
          df.filter(col("pump_flag"))
            .withColumn("severity_score",col("returns")*col("taker_ratio")*(col("volume")/col("rolling_avg_volume")))
            .select("open_time","close","returns","taker_ratio","volume","rolling_avg_volume","severity_score")
            .orderBy(col("severity_score").desc())
            .limit(top_n)
            )
    