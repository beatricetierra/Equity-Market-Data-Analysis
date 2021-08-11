# Equity-Market-Data-Analysis
Build an end-to-end data pipeline to ingest and process daily stock market data from multiple stock exchanges.
This project uses PySpark and Azure Blob Storage to process a small sample of stock market data (given) and transform it into more dynamic tables to easily run more complex queries. 

# Main Scripts
1. DataIngestion: Load files from blob storage
2. DataLoad: Clean downloaded data and upload to blob storage.
3. AnalyticalETL: Transforms data into new tables and uploads to blob storage.
4. Tracker: Tracks results of each pipeline step.

# Pipeline
The following diagrame describes the overall data flow.
![alt text](https://github.com/beatricetierra/Equity-Market-Data-Analysis/blob/main/FlowDiagram.png)

## DataIngestion
1. Reads raw data in csv and json formats from "data" folder.
2. Partitions based on record type (trade vs quote).
3. Uploads partitioned data in parquet files.

## DataLoad
1. Reads partitioned parquet files.
2. Adds "last_arrival_tm" column to sort data in case overlap occurred between transactions. 
3. Uploads corrected data back to blob storage under the "trade" or "quote" folder according to record type. 

## AnalyticalETL
1. Calculate 30-min moving average from trade table.
2. Adds 'last_pr' column to the trade table to list the last trade price from the previous day.
3. Combine quote and trade tables and fill-in all NULL values.
4. Filter quote records.
5. Uploads transformed data back to blob storage under "quote-trade-analytical" folder.

## Tracker
1. Keeps track of the process running all scripts above.
2. Records:
    - PartitionKey: stage of pipeline (DataIngestion, DataLoad, or AnalyticalETL)
    - RowKey: unique row id
    - Timestamp: time job was run
    - JobId: stage of pipeline + date
    - Status: starting, running, success, or failed
    - Error: Error message if job fails. Null if else. 
3. Updated in same blob storage table:
![alt text](https://github.com/beatricetierra/Equity-Market-Data-Analysis/blob/main/JobTrackerOutput.PNG)
