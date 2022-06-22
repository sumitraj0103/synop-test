# Databricks notebook source
# MAGIC %run "../fw/cmmn/config/environment_config"

# COMMAND ----------

# MAGIC %run "../fw/cmmn/module/mdle_config"

# COMMAND ----------

# MAGIC %run "../fw/cmmn/module/mdle_conn"

# COMMAND ----------

import json
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType
from datetime import datetime
import time
from pyspark.sql.window import Window

sampleDataPath = "/FileStore/data/sampleDF.csv"
sampleDF = spark.read.option("header", "true").csv(sampleDataPath).select(col("id"),col("amount"),col("credit_number"),col("data_dt"),col("event_code"),col("test_number"))

# COMMAND ----------

#struct(sampleDF.columns)

# COMMAND ----------

#struct("amount","credit_number","data_dt","event_code", "test_number")

# COMMAND ----------

# MAGIC %run "../test/module/mdle_strm_writer"

# COMMAND ----------

val_col = struct("amount","credit_number","data_dt","event_code", "test_number")
writeConfluent(sampleDF, "id", val_col, SECRET_SCOPE, "pkc-l9wvm.ap-southeast-1.aws.confluent.cloud:9092", "aws-confluent-test", "test01")

# COMMAND ----------

config_path = CLPP_CONFIG_PATH
predefined_connection = PREFEF_CONFIG_FILE

# read config
predefined_connection_config = PredefinedConnectionConfig(predefined_connection, config_path).get_config_df()

# inital input stream
input_stream = ConnectionFactory(c, predefined_connection_config).create().get_body_df()

# write stream (raw path)
sr = StreamToRaw(input_stream, c.raw.path)
sr.write()

# pre-process
pre = PreProcess(input_stream, c.output, c, predefined_connection_config)
ss = StreamToStream(pre)
body_df = ss.start()

# COMMAND ----------


