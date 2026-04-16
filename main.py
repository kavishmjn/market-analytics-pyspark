import time
from src.session import get_spark
from src.ingest import load_ohlcv
from src.transformations import add_lag_metrics, transform, add_core_metrics, add_rolling_metrics
from src.pump_detection import apply_pump_flags, get_top_pumps,compute_thresholds

spark = get_spark()
df = load_ohlcv(spark, "data/raw/")
df = transform(df)
df = add_core_metrics(df)
df = add_rolling_metrics(df)
df = add_lag_metrics(df)
threshold = compute_thresholds(df)
df = apply_pump_flags(df,threshold)
df = get_top_pumps(df, top_n=10)





start = time.time()
#df.explain("formatted")
row_count = df.count()
df.show(10,truncate=False)
end = time.time()
print(f"RUN TIME: {end - start:.2f}s")
print(f"ROW_COUNT: {row_count}")
print(f"Partitions: {df.rdd.getNumPartitions()}")
