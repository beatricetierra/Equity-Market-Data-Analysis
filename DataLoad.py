from pyspark.sql import SparkSession
from pyspark.sql.functions import max

def applyLatest(trade):
    latest_arrival_tm = trade.groupBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb").agg(max("arrival_tm").alias("latest"))
    return latest_arrival_tm

spark = SparkSession.builder.appName("app").getOrCreate()
spark.conf.set("fs.azure.account.key.equitymarketdatastorage.blob.core.windows.net", \
    "0Pjp/4C3REg7xPeNZulrdlcm85uSgj3mtonuvHyZcxNkDtvUyDmDqaum2rDj9qxucgJgHpLfDKCstiQ3UsMo8Q==")

# read trade partition
trade_common = spark.read.parquet("output_dir/partition=T")
trade = trade_common.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", \
                            "trade_pr", "bid_size")

# read quote parition
quote_common = spark.read.parquet("output_dir/partition=Q")
quote = quote_common.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", \
                            "bid_pr", "bid_size", "ask_pr", "ask_size")

# run applyLatest on both dataframes
trade_corrected = applyLatest(trade)
quote_corrected = applyLatest(quote)

# export to azure blob storage
cloud_storage_path = "wasbs://market-value@equitymarketdatastorage.blob.core.windows.net/output_dir/"
trade_corrected.write.parquet(cloud_storage_path + "trade.parquet")
quote_corrected.write.parquet(cloud_storage_path + "quote.parquet")