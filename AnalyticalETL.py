from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import datetime

spark = SparkSession.builder.appName("app").getOrCreate()
spark.conf.set("fs.azure.account.key.equitymarketdatastorage.blob.core.windows.net", \
    "0Pjp/4C3REg7xPeNZulrdlcm85uSgj3mtonuvHyZcxNkDtvUyDmDqaum2rDj9qxucgJgHpLfDKCstiQ3UsMo8Q==")

# read cleaned data into temp tamle
cloud_storage_path = "wasbs://market-value@equitymarketdatastorage.blob.core.windows.net/output_dir/"
trade_df = spark.read.parquet(cloud_storage_path + "trade/date={dt}".format(dt='2020-08-05'))
trade_df.createOrReplaceTempView("trades")

# select columns into temp table
df = spark.sql("select symbol, exchange, event_tm, event_seq_nb, trade_pr from trades \
where trade_dt = '2020-08-05'")
df.createOrReplaceTempView("tmp_trade_moving_avg")

# 30-min moving average
mov_avg_df = spark.sql("""
SELECT symbol, exchange, event_tm, event_seq_nb, trade_pr,
avg(trade_pr) OVER(PARTITION BY symbol ORDER BY CAST(event_tm AS timestamp) 
    RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as mov_avg_pr
FROM tmp_trade_moving_avg
""")
mov_avg_df.createOrReplaceTempView("tmp_trade_moving_avg")

# same process on previous date 
date = datetime.datetime.strptime('2020-08-05', '%Y-%m-%d')
prev_date_str = str(date.date() - datetime.timedelta(days=1))

df = spark.sql("select symbol, exchange, event_tm, event_seq_nb, trade_pr from trades \
where trade_dt = '{}'".format('2020-08-05'))
df.createOrReplaceTempView("tmp_last_trade")

last_pr_df = spark.sql("""
SELECT symbol, exchange, trade_pr AS last_pr 
FROM tmp_last_trade t1
JOIN (SELECT MAX(event_tm) AS last_record FROM tmp_last_trade GROUP BY symbol, exchange) t2
    ON t1.event_tm == last_record
""")
last_pr_df.createOrReplaceTempView("tmp_last_trade")

# # Define new schema
# schema = StructType([ \
#     StructField("trade_dt", StringType(), True), \
#     StructField("rec_type", StringType(),True), \
#     StructField("symbol", StringType(),True), \
#     StructField("exchange", StringType(), True), \
#     StructField("event_tm", StringType(), True), \
#     StructField("event_seq_nb", StringType(), True), \
#     StructField("bid_pr", StringType(),True), \
#     StructField("bid_size", StringType(), True), \
#     StructField("ask_pr", StringType(), True), \
#     StructField("ask_size", StringType(), True), \
#     StructField("trade_pr", StringType(),True), \
#     StructField("mov_avg_pr", StringType(), True) 
#   ])

# Get quotes table
quote_df = spark.read.parquet(cloud_storage_path + "quote/date={dt}".format(dt='2020-08-05'))
quote_df.createOrReplaceTempView("quotes")

# Join both tables
quote_union = spark.sql("""
SELECT *
FROM quotes
LEFT JOIN tmp_last_trade trade
    ON quotes.symbol == trade.symbol AND quotes.exchange == trade.exchange
""")
quote_union.show(100)
# quote_union.createOrReplaceTempView("quote_union")
