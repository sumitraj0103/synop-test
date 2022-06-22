# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Unit Test

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Unit Test: Data Pre Processing
# MAGIC 
# MAGIC | Test ID |            Category           |                     Scenario                     |             Test Data            |                 Expected Result                | Actual Result | Pass/Fail |
# MAGIC |:-------:|:-----------------------------:|:------------------------------------------------:|:--------------------------------:|:----------------------------------------------:|:-------------:|:---------:|
# MAGIC | P1      | Predefined input config       | Correct config format                            | predefined_connections_refactor_v1.json   | Read all fields to DF                          | As Expected   | Pass      |
# MAGIC | P1.1    | Predefined input config       | Wrong config format               | predefined_connections_refactor_v1.json   | Error with exception  | As Expected   | Pass      |
# MAGIC | P2      | Job input config       | Correct config format | job_config_preprocess_refactor_v1.json   | Read all fields to DF |As Expected|Pass|
# MAGIC | P2.1      | Job input config       | Wrong config format | job_config_preprocess_refactor_v1.json   | Error with exception |As Expected|Pass|
# MAGIC | P3      | Read stream and write to ADLS | Confluent single stream                          | job_config_preprocess_refactor_v1.json, redefined_connections_refactor_v1.json| count = 202                                      |As Expected|Pass|
# MAGIC | P4      | Read stream and write to Internal EventHub | Check schema in Internal EventHub  | job_config_preprocess_refactor_v1.json, redefined_connections_refactor_v1.json | [$.body.event_code, $.body.amt, $.body.credit_card_number, $.body.data_dt, $.body.time_constant, $.body.event_id]                        |As Expected|Pass|

# COMMAND ----------

# MAGIC %run "../src/fw_data_collection_and_preprocessing" $job_id="preprocess_refactor_v1" $predefined_connection ="predefined_connections_refactor_v1" $debug="True"

# COMMAND ----------

sampleDF = spark.read.format("delta").load("/delta/sampledata").cache()

def sendSample():
  writeQuery = (
  sampleDF.select(col('id').alias('key'), to_json(struct("amount","credit_number","data_dt","event_code", "test_number")).alias('value')) \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "pkc-l9wvm.ap-southeast-1.aws.confluent.cloud:9092") \
  .option("topic", "test01") \
  .option("kafka.security.protocol","SASL_SSL")\
  .option("kafka.sasl.mechanism", "PLAIN")\
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"YYPGB4ZHMBOQDPCN\" password=\"FV9CweH7w+mcZxk8PLJaGYFba5HfV+xVCk1BHN52i0MTA/KOutICfLlufQTvj6S/\";") \
  .save()
)
  
def get_df_from_eventhub(connectionString):
  # Create the starting position Dictionary
  startingEventPosition = {
    "offset": None,         # not in use 
    "seqNo": -1,            # not in use
    "enqueuedTime": datetime.now().isoformat()+'Z',
    "isInclusive": True
  }

  eventHubsConf = {
    "eventhubs.connectionString" : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
    "eventhubs.startingPosition" : json.dumps(startingEventPosition),
    "setMaxEventsPerTrigger": 100,
    "consumerGroup": "dataplatform" #TODO: make this auto
  }

  spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

  eventStreamDF = (spark.readStream
    .format("eventhubs")
    .options(**eventHubsConf)
    .load()
  )
  bodyDF = eventStreamDF.select(col("body").cast("STRING"))
  
  return bodyDF
  
def stream_viewer(connection_string):
  return get_df_from_eventhub(connection_string)

# COMMAND ----------

import unittest, logging
from py4j.protocol import *
from pyspark.sql.utils import *
from pyspark.sql.types import *

class TestPredefinedConfig(unittest.TestCase):
  
  def test_correct_config(self):
    config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/preprocess/"
    predefined_connection = "predefined_connections_refactor_v1"
    
    got = PredefinedConnectionConfig(predefined_connection, config_path).get_config_df().schema.names
    want = ["type", "config"]
                
    self.assertEqual(got, want)
      
  def test_wrong_config_path(self):
    config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/wrong/"
    predefined_connection = "predefined_connections_refactor_v1"
    with self.assertRaises(AnalysisException):
      PredefinedConnectionConfig(predefined_connection, config_path).get_config_df()
      
      
class TestJobIDConfig(unittest.TestCase):
  
  def test_correct_config(self):
    config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/preprocess/"
    job_id = "preprocess_refactor_v1"
    got = JobConfig(job_id, config_path).get_config_df().schema.names
    want = ['input', 'output', 'raw', 'pre_process']
    
    self.assertEqual(got ,want)
    
  def test_wrong_config_path(self):
    config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/wrong/"
    job_id = "predefined_connections_refactor_v1"
    with self.assertRaises(AnalysisException):
      JobConfig(job_id, config_path).get_config_df()
    

class TestReadWriteADLS(unittest.TestCase):
  
  def test_read_stream_and_write_to_adls(self):
    config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/preprocess/"
    job_id = "preprocess_refactor_v1"
    predefined_connection = "predefined_connections_refactor_v1"
    job_id_config = JobConfig(job_id, config_path).get_config_df()
    predefined_connection_config = PredefinedConnectionConfig(predefined_connection, config_path).get_config_df()
    input_stream = ConnectionFactory(job_id_config.collect()[0], predefined_connection_config).create().get_body_df()
    sr = StreamToRaw(input_stream, job_id_config.collect()[0].raw.path)
    stream = sr.write()
    
    sendSample()
    got = spark.read.parquet("/mnt/lake/raw/aws_confluent_test_test02_v1").count()
    
    want = 202
    
    self.assertEqual(got, want)

class TestPreProcessing(unittest.TestCase):
  
  def test_read_stream_and_write_to_internal_event_hub(self):
    config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/preprocess/"
    job_id = "preprocess_refactor_v1"
    predefined_connection = "predefined_connections_refactor_v1"
    job_id_config = JobConfig(job_id, config_path).get_config_df()
    predefined_connection_config = PredefinedConnectionConfig(predefined_connection, config_path).get_config_df()
    input_stream = ConnectionFactory(job_id_config.collect()[0], predefined_connection_config).create().get_body_df()
    pre = PreProcess(input_stream, job_id_config.collect()[0].output, job_id_config.collect()[0], predefined_connection_config)
    ss = StreamToStream(pre)
    #ss.start()
    
    df = stream_viewer("Endpoint=sb://dbfwpoc.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=EKyOWJIS0NAo0iwagcEy9ApLCkAHtDaYbrp/3e7uujQ=;EntityPath=desteventhub")
    display(df)

test_cases = (TestPredefinedConfig, TestJobIDConfig, TestReadWriteADLS, TestPreProcessing)

def load_tests():
    runner = unittest.TextTestRunner(verbosity=2)
    for test_class in test_cases:
      print(test_class)
      suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
      runner.run(suite)
    
load_tests()

# COMMAND ----------


