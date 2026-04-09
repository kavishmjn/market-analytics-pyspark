from src.session import get_spark
from src.ingest import load_ohlcv
from src.transformations import add_lag_metrics, transform,add_core_metrics,add_rolling_metrics
from src.pump_detection import add_pump_flags,get_top_pumps
import pyspark.sql.functions as F
spark = get_spark()

df = load_ohlcv(spark, "data/raw/")

#print(f"Total rows: {df.count()}")
#print(f"Partitions: {df.rdd.getNumPartitions()}")

df.printSchema()
df.show(5)

tranformed_time_df  = transform(df)
#tranformed_time_df.printSchema()
#tranformed_time_df.show(5)


core_metrics_df = add_core_metrics(tranformed_time_df)
#core_metrics_df.printSchema()
#core_metrics_df.show(5)

rolling_metrics_df = add_rolling_metrics(core_metrics_df)
#rolling_metrics_df.printSchema()
#rolling_metrics_df.show(5)

lag_metrics_df = add_lag_metrics(rolling_metrics_df)
#lag_metrics_df.printSchema()   
#lag_metrics_df.show(5)

pump_flags_df = add_pump_flags(lag_metrics_df)
pump_flags_df.printSchema()
pump_flags_df.show(5)

top_pumps_df = get_top_pumps(pump_flags_df)
top_pumps_df.printSchema()
top_pumps_df.show(5)


