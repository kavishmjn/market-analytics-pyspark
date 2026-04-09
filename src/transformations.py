from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp,avg,lag,stddev
from pyspark.sql.window import Window



def transform(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("open_time", to_timestamp(col("open_time").cast("double") / 1_000_000))
        .withColumn("close_time", to_timestamp(col("close_time").cast("double") / 1_000_000))
    )



def add_core_metrics(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("returns",(col("close")-col("open"))/col("open"))
          .withColumn("price_range",col("high")-col("low"))
          .withColumn("typical_price",(col("high")+col("low")+col("close"))/3)
          .withColumn("taker_ratio",col("taker_buy_volume")/col("volume"))
          .withColumn("vwap_simple",col("quote_volume")/col("volume"))
    )

#Function 1 — add_rolling_metrics(df)
	#∙	rolling_avg_volume — average volume over last 20 candles
	#∙	rolling_volatility — standard deviation of returns over last 20 candles
	#∙	rolling_vwap — average of vwap_simple over last 7 candles

def add_rolling_metrics(df: DataFrame) -> DataFrame:
    window_20 = Window.partitionBy("symbol").orderBy("open_time").rowsBetween(-19,0)
    window_7 = Window.partitionBy("symbol").orderBy("open_time").rowsBetween(-6,0)
    return (
        df.withColumn('rolling_avg_volume',avg(col('volume').cast('double')).over(window_20))
          .withColumn('rolling_volatility',stddev(col('returns')).over(window_20))
          .withColumn('rolling_vwap',avg(col('vwap_simple')).over(window_7))
          )
    
#Function 2 — add_lag_metrics(df)
	#∙	prev_close — close price of the previous candle (1 row back)
	#∙	price_momentum — close price 5 candles ago
    #.	consecutive_green — was the previous candle also green? (close > open)

def add_lag_metrics(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("symbol").orderBy("open_time")
    return (
        df
        .withColumn('prev_close', lag(col('close'), 1).over(window))
        .withColumn('price_momentum', lag(col('close'), 5).over(window))
        .withColumn('consecutive_green',
            (col('close') > col('open')) &
            (lag(col('close'), 1).over(window) > lag(col('open'), 1).over(window))
        )
    )