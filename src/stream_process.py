from session import get_spark
from ingest import FILE_SCHEMA, load_ohlcv
from transformations import add_lag_metrics, transform, add_core_metrics, add_rolling_metrics
from pump_detection import compute_thresholds, apply_pump_flags
from pyspark.sql.functions import col, input_file_name, regexp_extract

STREAMING_PATH = 'data/streaming'
HISTORICAL_PATH = 'data/raw/'

def clean_df(df):
    return df.drop("ignore").withColumn(
        "symbol", regexp_extract(input_file_name(), r"([A-Z]+)_([0-9]+)_([0-9]+)", 1)
    )

def run():
    spark = get_spark()

    # compute thresholds once from historical data at startup
    #Historical data has thousands of rows per symbol — mean and stddev computed from that are statistically meaningful and stable.
    #Every micro-batch then uses those same reliable thresholds instead of trying to compute statistics from 2 rows which would be meaningless.
    #The broadcast ensures those thresholds are sent to all partitions efficiently rather than shuffled. 
    #Same pattern you learned in M6, just now the thresholds are computed once at startup instead of once per batch.
    print("[INFO] Loading historical data and computing thresholds...")
    hist_df = load_ohlcv(spark, HISTORICAL_PATH)
    hist_df = transform(hist_df)
    hist_df = add_core_metrics(hist_df)
    hist_df = add_rolling_metrics(hist_df)
    hist_df = add_lag_metrics(hist_df)
    thresholds = compute_thresholds(hist_df)
    print("[INFO] Thresholds computed:")
    thresholds.show()

    def process_stream(batch_df, batch_id):
        if batch_df.count() == 0:
            print(f"[INFO] Batch {batch_id} is empty, skipping.")
            return
        print(f"\n{'='*50}")
        print(f"Micro-batch {batch_id} — {batch_df.count()} new rows")
        df = clean_df(batch_df)
        df = transform(df)
        df = add_core_metrics(df)
        df = add_rolling_metrics(df)
        df = add_lag_metrics(df)
        df = apply_pump_flags(df, thresholds)
        pumps = df.filter(col('pump_flag'))
        pump_count = pumps.count()
        if pump_count > 0:
            print(f"PUMP DETECTED!!: {pump_count} in batch {batch_id}")
            pumps.select(
                "symbol", "open_time_utc", "close",
                "returns", "taker_ratio", "volume",
                "severity_score"
            ).show(truncate=False)
        else:
            print(f"No pumps detected in batch {batch_id}.")

    stream_df = (
        spark.readStream
            .schema(FILE_SCHEMA)
            .option("header", "true")
            .option("maxFilesPerTrigger", 5)
            .option("recursiveFileLookup", "true")
            .csv(STREAMING_PATH)
    )

    query = (
        stream_df.writeStream
            .foreachBatch(process_stream)
            .trigger(processingTime='60 seconds')
            .option("checkpointLocation", "output/checkpoints/streaming")
            .start()
    )

    print("Stream running... watching", STREAMING_PATH)
    query.awaitTermination()


if __name__ == "__main__":
    run()