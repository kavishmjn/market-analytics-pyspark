from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,input_file_name,regexp_extract
from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType
)

FILE_SCHEMA = StructType([
    StructField("open_time",        LongType(),   False),
    StructField("open",             DoubleType(), False),
    StructField("high",             DoubleType(), False),
    StructField("low",              DoubleType(), False),
    StructField("close",            DoubleType(), False),
    StructField("volume",           DoubleType(), False),
    StructField("close_time",       LongType(),   False),
    StructField("quote_volume",     DoubleType(), False),
    StructField("trade_count",      LongType(),   False),
    StructField("taker_buy_volume", DoubleType(), False),
    StructField("taker_buy_quote",  DoubleType(), False),
    StructField("ignore",           DoubleType(), True),
])

def load_ohlcv(spark: SparkSession, path: str):
    df = spark.read.csv(
        path,
        schema=FILE_SCHEMA,
        header=False
    )
    file_name = regexp_extract(input_file_name(), r"([A-Z]+)-1m-", 1)
    df = df.drop("ignore").withColumn("symbol", file_name)  
    return df