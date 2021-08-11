from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DecimalType
from pyspark.sql import SparkSession
from typing import List
import json
import os
from glob import glob
from Tracker import JobTracker

def find_files(format):
    path = '.\\data\\'
    files = [file for path, subdir, files in os.walk(path) \
        for file in glob(os.path.join(path, format+'/*/*/*.txt'))]
    return files 

def common_event(*args):
    return [arg for arg in args]

def parse_csv(line:str):
    record_type_pos = 2
    record = line.split(",")
    try:
        # [logic to parse records]
        if record[record_type_pos] == "Q":
            event = common_event(record[0], record[2], record[3], record[6], record[4], record[5], \
                                  record[1], None, record[7], record[8], record[9], record[10], "Q")
            return event
        elif record[record_type_pos] == "T":
            event = common_event(record[0], record[2], record[3], record[6], record[4], record[5], \
                                  record[1], record[7], None, record[8], None, None, "T")
            return event
    except Exception as e:
        # [save record to dummy event in bad partition]
        # [fill in the fields as None or empty string]
        return common_event(None,None,None,None,None,None,None,None,None,None,None,None,"B")

def parse_json(line:str):
    record = json.loads(line)
    record_type = record['event_type']
    try:
        # [logic to parse records]
        if record_type == "T":
            # [Get the applicable field values from json]
            if 'execution_id' in record:
                event = common_event(record['trade_dt'], record['event_type'], record['symbol'], record['exchange'], record['event_tm'], \
                                    record['event_seq_nb'], record['file_tm'], record['price'], None, record['size'], None, None,"T")
            else:
                event = common_event(None,None,None,None,None,None,None,None,None,None,None,None,"B")
            return event
        elif record_type == "Q":
            # [Get the applicable field values from json]
            if 'event_seq_nb' in record:
                event = common_event(record['trade_dt'], record['event_type'], record['symbol'], record['exchange'], record['event_tm'], \
                                    record['event_seq_nb'], record['file_tm'], None, record['bid_pr'], record['bid_size'], record['ask_pr'], record['ask_size'],"Q")
            else:
                event = common_event(None,None,None,None,None,None,None,None,None,None,None,None,"B")
            return event
    except Exception as e:
        # [save record to dummy event in bad partition]
        # [fill in the fields as None or empty string]
        return common_event(None,None,None,None,None,None,None,None,None,None,None,None,"B")

# Start DataIngestion
tracker = JobTracker()
tracker.update_job_status('DataIngestion', 'starting')

spark = SparkSession.builder.master("local").appName("app").getOrCreate()
spark.conf.set("fs.azure.account.key.equitymarketdatastorage.blob.core.windows.net",\
    "DefaultEndpointsProtocol=https;AccountName=equitymarketdatastorage;AccountKey=0Pjp/4C3REg7xPeNZulrdlcm85uSgj3mtonuvHyZcxNkDtvUyDmDqaum2rDj9qxucgJgHpLfDKCstiQ3UsMo8Q==;EndpointSuffix=core.windows.net")
schema = StructType([ \
    StructField("trade_dt", StringType(), True), \
    StructField("rec_type", StringType(),True), \
    StructField("symbol", StringType(),True), \
    StructField("exchange", StringType(), True), \
    StructField("event_tm", StringType(), True), \
    StructField("event_seq_nb", StringType(), True), \
    StructField("arrival_tm", StringType(), True), \
    StructField("trade_pr", StringType(),True), \
    StructField("bid_pr", StringType(),True), \
    StructField("bid_size", StringType(), True), \
    StructField("ask_pr", StringType(), True), \
    StructField("ask_size", StringType(), True), \
    StructField("partition", StringType(), True) \
  ])

try:
    # Access blob storage
    tracker.update_job_status('DataIngestion', 'running')

    csv_raw = spark.sparkContext.textFile("wasbs://market-value@equitymarketdatastorage.blob.core.windows.net/data/csv/2020-08-05/NYSE/part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.txt")
    csv_parsed = csv_raw.map(lambda line: parse_csv(line))
    csv_data = spark.createDataFrame(csv_parsed, schema=schema)
    csv_data.show()

    json_raw = spark.sparkContext.textFile("wasbs://market-value@equitymarketdatastorage.blob.core.windows.net/data/json/2020-08-05/NASDAQ/part-00000-c6c48831-3d45-4887-ba5f-82060885fc6c-c000.txt")
    json_parsed = json_raw.map(lambda line: parse_json(line))
    json_data = spark.createDataFrame(json_parsed, schema=schema)
    json_data.show()

    # Save output
    csv_data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")
    json_data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")
    tracker.update_job_status('DataIngestion', 'success')
except Exception as e:
    tracker.update_job_status('DataIngestion', 'failed', str(e))