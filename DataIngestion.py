from pyspark.sql import SparkSession
from typing import List
import json

def common_event():
    return

# def parse_csv(line:str):
#     record_type_pos = 2
#     record = line.split(",")
#     try:
#         # [logic to parse records]
#         if record[record_type_pos] == "T":
#             event = common_event(col1_val, col2_val, ..., "T","")
#             return event
#         elif record[record_type_pos] == "Q":
#             event = common_event(col1_val, col2_val, … , "Q","")
#             return event
#     except Exception as e:
#         # [save record to dummy event in bad partition]
#         # [fill in the fields as None or empty string]
#         return common_event(,,,....,,,,,"B",line)

# def parse_json(line:str):
#     record_type = record['event_type']
#     try:
#         # [logic to parse records]
#         if record_type == "T":
#             # [Get the applicable field values from json]
#             if # [some key fields empty]:
#                 event = common_event(col1_val, col2_val, ..., "T","")
#             else:
#                 event = ommon_event(,,,....,,,,,"B",line)
#             return event
#         elif record_type == "Q":
#             # [Get the applicable field values from json]
#             if # [some key fields empty]:
#                 event = common_event(col1_val, col2_val, … , "Q","")
#             else:
#                 event = common_event(,,,....,,,,,"B",line)
#             return event
#     except Exception as e:
#         # [save record to dummy event in bad partition]
#         # [fill in the fields as None or empty string]
#         return common_event(,,,....,,,,,"B",line)

spark = SparkSession.builder.master("local").appName("app").getOrCreate()
spark.conf.set("fs.azure.account.key.equitymarketdatastorage.blob.core.windows.net","0Pjp/4C3REg7xPeNZulrdlcm85uSgj3mtonuvHyZcxNkDtvUyDmDqaum2rDj9qxucgJgHpLfDKCstiQ3UsMo8Q==")
raw = spark.sparkContext.textFile("market-value/data/csv/2020-08-05/NYSE/part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.txt")
#parsed = raw.map(lambda line: parse_csv(line))
#data = spark.createDataFrame(parsed)
#data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")
