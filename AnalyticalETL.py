from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import datetime
from Tracker import JobTracker

# Connect to spark
tracker = JobTracker()
tracker.update_job_status('AnalyticalETL', 'starting')

spark = SparkSession.builder.appName("app").getOrCreate()
spark.conf.set("fs.azure.account.key.equitymarketdatastorage.blob.core.windows.net", \
    "0Pjp/4C3REg7xPeNZulrdlcm85uSgj3mtonuvHyZcxNkDtvUyDmDqaum2rDj9qxucgJgHpLfDKCstiQ3UsMo8Q==")

try:
    ############ 1. Create tmp_trade_moving_avg table ############
    tracker.update_job_status('AnalyticalETL', 'running')
    # Read trade parquet files
    cloud_storage_path = "wasbs://market-value@equitymarketdatastorage.blob.core.windows.net/output_dir/"
    trade_df = spark.read.parquet(cloud_storage_path + "trade/date={dt}".format(dt='2020-08-05'))
    trade_df.createOrReplaceTempView("trades")

    # Select columns into temp table
    df = spark.sql("select trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr from trades \
    where trade_dt = '2020-08-05'")
    df.createOrReplaceTempView("tmp_trade_moving_avg")

    # Calculate 30-min moving average
    mov_avg_df = spark.sql("""
    SELECT trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr,
    avg(trade_pr) OVER(PARTITION BY symbol ORDER BY CAST(event_tm AS timestamp) 
        RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as mov_avg_pr
    FROM tmp_trade_moving_avg
    """)
    mov_avg_df.createOrReplaceTempView("tmp_trade_moving_avg")

    ############# 2. Create last_pr table (last trade price from previous day) ############

    # Get yesterday's date
    date = datetime.datetime.strptime('2020-08-05', '%Y-%m-%d')
    prev_date_str = str(date.date() - datetime.timedelta(days=1))

    # Select columns into temp table
    df = spark.sql("select symbol, exchange, event_tm, event_seq_nb, trade_pr from trades \
    where trade_dt = '{}'".format('2020-08-05'))
    df.createOrReplaceTempView("tmp_last_trade")

    # Get last listing price per symbol, exchange
    last_pr_df = spark.sql("""
    SELECT symbol, exchange, trade_pr AS close_pr
    FROM tmp_last_trade t1
    JOIN (SELECT MAX(event_tm) AS last_record FROM tmp_last_trade GROUP BY symbol, exchange) t2
        ON t1.event_tm == last_record
    """)
    last_pr_df.createOrReplaceTempView("tmp_last_trade")

    ############# 3. Read quotes table ############

    quote_df = spark.read.parquet(cloud_storage_path + "quote/date={dt}".format(dt='2020-08-05'))
    quote_df.createOrReplaceTempView("quotes")

    ############# 4. Union quotes (step 3) and tmp_trade_moving_avg (step 1) ############
    quote_union = spark.sql("""
    SELECT trade_dt, symbol, exchange, event_tm, event_seq_nb, NULL as arrival_tm, NULL as bid_size, 
        NULL as ask_pr, NULL as ask_size, NULL as latest, trade_pr, mov_avg_pr from tmp_trade_moving_avg
    Union
    Select trade_dt, symbol, exchange, event_tm, event_seq_nb, arrival_tm, bid_size, ask_pr, ask_size, latest, 
        NULL as trade_pr, NULL as mov_avg_pr from quotes
    """)
    quote_union.createOrReplaceTempView("quote_union")

    ############# 5. Populate latest trade_pr and mov_avg_pr ############
    quote_union_update = spark.sql("""
    SELECT quote_union.trade_dt, quote_union.symbol, quote_union.exchange, quote_union.event_tm, 
            quote_union.event_seq_nb, quote_union.arrival_tm, quote_union.bid_size, quote_union.ask_pr, 
            quote_union.ask_size, quote_union.latest, latest_trade.latest_trade_pr, latest_trade.mov_avg_pr
    FROM quote_union
    JOIN (SELECT quote.symbol, quote.exchange, quote.trade_pr as latest_trade_pr, quote.mov_avg_pr
        FROM quote_union quote
        JOIN (SELECT MAX(event_tm) as latest_event_tm FROM quote_union GROUP BY symbol, exchange) last_trade_tm
            ON quote.event_tm == last_trade_tm.latest_event_tm) latest_trade
        ON quote_union.symbol == latest_trade.symbol AND quote_union.exchange == latest_trade.exchange
    """)
    quote_union_update.createOrReplaceTempView("quote_union_update")

    ############# 6. Filter for only quote records ############
    quote_update = spark.sql("""
    SELECT *
    FROM quote_union_update
    WHERE ask_size IS NOT NULL
    """)
    quote_update.createOrReplaceTempView("quote_update")

    ############# 6. Filter for only quote records ############
    quote_final = spark.sql("""
    SELECT /*+ BROADCAST(tmp_last_trade) */ * 
    FROM quote_update 
    LEFT JOIN tmp_last_trade
        USING(symbol, exchange)
    """)

    ############# 7. Export to blob storage in parquet format ############
    cloud_storage_path = "wasbs://market-value@equitymarketdatastorage.blob.core.windows.net/quote-trade-analytical/"
    quote_final.write.parquet(cloud_storage_path + "trade/date={dt}".format(dt=str(date.date())))
    tracker.update_job_status('AnalyticalETL', 'success')

except Exception as e:
    tracker.update_job_status('AnalyticalETL', 'failed', str(e))