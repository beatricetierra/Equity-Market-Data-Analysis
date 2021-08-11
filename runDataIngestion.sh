#!/bin/bash
spark-submit DataIngestion.py --master local --jars jars/postgresql-42.2.14.jar, jars/hadoop-azure.jar, jars/azure-storage.jar --py-files Tracker.py
spark-submit DataLoad.py --master local --jars jars/postgresql-42.2.14.jar, jars/hadoop-azure.jar, jars/azure-storage.jar --py-files Tracker.py