#!/bin/bash
spark-submit AnalyticalETL.py \
--master local \
--jars jars/postgresql-42.2.14.jar, jars/hadoop-azure.jar, jars/azure-storage.jar