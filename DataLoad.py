from pyspark.sql import SparkSession
from pyspark.sql.functions import max

def applyLatest(trade):
    latest_arrival_tm = trade.groupBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb").agg(max("arrival_tm"))
    return latest_arrival_tm

spark = SparkSession.builder.master("local").appName("app").getOrCreate()
spark.conf.set("fs.azure.account.key.equitymarketdatastorage.blob.core.windows.net", \
    "DefaultEndpointsProtocol=https;AccountName=equitymarketdatastorage;AccountKey=0Pjp/4C3REg7xPeNZulrdlcm85uSgj3mtonuvHyZcxNkDtvUyDmDqaum2rDj9qxucgJgHpLfDKCstiQ3UsMo8Q==;EndpointSuffix=core.windows.net")

# read trade partition
trade_common = spark.read.parquet("output_dir/partition=T")
trade = trade_common.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", \
                            "trade_pr", "bid_size")

# read quote parition
# quote_common = spark.read.parquet("output_dir/partition=Q")
# quote = quote_common.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", \
#                             "bid_pr", "bid_size", "ask_pr", "ask_size")

trade_corrected = applyLatest(trade)

trade_date = "2021-07-30"
trade_corrected.write.parquet("wasbs://market-value@equitymarketdatastorage.blob.core.windows.net/")