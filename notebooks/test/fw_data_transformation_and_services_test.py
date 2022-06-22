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
# MAGIC | 1      | Predefined input config       | Correct config format                            | predefined_connections_refactor_v1.json   | Read all fields to DF                          | As Expected   | Pass      |
# MAGIC | 2    | Predefined input config       | Wrong config format               | predefined_connections_refactor_v1.json   | Error with exception  | As Expected   | Pass      |
# MAGIC | 3      | Job input config       | Correct config format | job_config_marketing.json   | Read all fields to DF |As Expected|Pass|
# MAGIC | 4      | Job input config       | Wrong config format | job_config_marketing.json   | Error with exception |As Expected|Pass|
# MAGIC | 5      | Data Transformation | Read stream to temp view correctly                          | job_config_marketing.json, redefined_connections_refactor_v1.json| count = 15                                      |As Expected|Pass|
# MAGIC | 5      | Data Transformation | Transform to temp view correctly                         | job_config_marketing.json, redefined_connections_refactor_v1.json| count = 4, filtered only SE customer                                       |As Expected|Pass|
# MAGIC | 5      | Data Transformation | Write to target stream from temp view correctly                          | job_config_marketing.json, redefined_connections_refactor_v1.json| count = 4, transformed view in target stream                                      |As Expected|Pass|

# COMMAND ----------

# MAGIC %run "../src/fw_data_transformation_and_services" $job_id="transform_marketing" $predefined_connection ="predefined_connections_refactor_v1" $debug="True"

# COMMAND ----------

from pyspark.sql.window import Window
cust_acct = spark.table('dl_curated_cust_db.cust_acct')
acct_num = cust_acct.limit(15).select(col('acct_num'))
sampleDF = spark.read.format("delta").load("/delta/sampledata").withColumn("index", row_number().over(Window.orderBy(lit(1)))) \
    .join(acct_num.withColumn("index2", row_number().over(Window.orderBy(lit(1)))), col("index") == col("index2")) \
    .drop("index2") \
    .cache() #TODO: write to delta path

def sendSample():
  connection_string = "Endpoint=sb://performancetest.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=DuBH3QvBVnuvdK35n5/JQcgskoi0W/p8nYt1RwHulMo=;EntityPath=ingestion"
  ehWriteConf = {
     'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
  }
  checkpointPath = "/preprocess/event-hub/write-checkpoint/input_sim"
  dbutils.fs.rm(checkpointPath, True)
  bodyDF = sampleDF.select(to_json(struct([sampleDF[x] for x in sampleDF.columns])).alias("body"))
  bodyDF.write.format("eventhubs").options(**ehWriteConf).option("checkpointLocation", checkpointPath).save()
  
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

# COMMAND ----------

import unittest, logging, time
from py4j.protocol import *
from pyspark.sql.utils import *
from pyspark.sql.types import *

def load_tests(test_cases):
    runner = unittest.TextTestRunner(verbosity=2)
    for test_class in test_cases:
      print(test_class)
      suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
      runner.run(suite)

# COMMAND ----------

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
  
  def test_correct_input_config(self):
    config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/transform/"
    job_id = "transform_marketing"
    
    got = TransformJobConfig(job_id, config_path).get_input_config_df().select("input.*").schema.names
    required = ['name', 'type', 'namespace', 'topic']

    self.assertTrue(set(required).issubset(got))
    
  def test_correct_transform_config(self):
    config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/transform/"
    job_id = "transform_marketing"
    
    got = TransformJobConfig(job_id, config_path).get_transform_config_df().select("transform.*").schema.names
    required = ['type', 'script']

    self.assertTrue(set(required).issubset(got))

  def test_correct_output_config(self):
    config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/transform/"
    job_id = "transform_marketing"
    
    got = TransformJobConfig(job_id, config_path).get_output_config_df().select("output.*").schema.names
    required = ['name', 'type', 'namespace', 'topic']

    self.assertTrue(set(required).issubset(got))

  def test_wrong_config_path(self):
    config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/wrong/"
    job_id = "predefined_connections_refactor_v1"
    with self.assertRaises(AnalysisException):
      TransformJobConfig(job_id, config_path).get_input_config_df()
    

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
    self.assertEqual(1,1)

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

class TestTransformation(unittest.TestCase): 
  def test_transformed_output(self):
    config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/transform/"
    job_id = "transform_marketing"
    predefined_connection = "predefined_connections_refactor_v1"
  
    # read config
    job_input_config = TransformJobConfig(job_id, config_path).get_input_config_df()
    job_transform_config = TransformJobConfig(job_id, config_path).get_transform_config_df()
    job_output_config = TransformJobConfig(job_id, config_path).get_output_config_df()
    predefined_connection_config = PredefinedConnectionConfig(predefined_connection, predefined_config_path).get_config_df()

    # read input
    for c in job_input_config.collect():
      input_stream = ConnectionFactory(c, predefined_connection_config).create().get_body_df()
      input_stream.createOrReplaceTempView(c.input.name)

    # execute transformation
    for c in job_transform_config.collect():
      transformation = Transform(c, script_path)
      transformation.execute()
    
    # write output
    for c in job_output_config.collect():
      output_stream = ViewToOutput(c, predefined_connection_config)
      output_stream.start()

    spark.table("logging_events") \
    .writeStream \
    .format("memory") \
    .queryName("got_input") \
    .start()
    
    spark.table("targeted_marketing_events") \
    .writeStream \
    .format("memory") \
    .queryName("got_transform") \
    .start()
    
    get_df_from_eventhub("Endpoint=sb://dbfwpoc.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=EKyOWJIS0NAo0iwagcEy9ApLCkAHtDaYbrp/3e7uujQ=;EntityPath=desteventhub") \
    .writeStream \
    .format("memory") \
    .queryName("got_output") \
    .start()
    
    time.sleep(60)
    
    sendSample()
    
    time.sleep(60)
    
    want_input = 15
    want_transform = 4
    want_output = 4
    
    got_input = spark.table("got_input").count()
    got_transform = spark.table("got_transform").count()
    got_output = spark.table("got_output").count()
    
    self.assertEqual(got_input, want_input)
    self.assertEqual(got_transform, want_transform)
    self.assertEqual(got_output, want_output)
    
test_cases = (TestPredefinedConfig, TestJobIDConfig, TestTransformation)
#test_cases = (TestPredefinedConfig, TestJobIDConfig)
    
load_tests(test_cases)

# COMMAND ----------

#sc.stop() #TODO: clear memory sink every test instead of restarting Spark Context

# COMMAND ----------

# MAGIC %md ## Data Services
# MAGIC 
# MAGIC **Data Services output channels**
# MAGIC - Event Hubs: Passed (above)
# MAGIC - Evergage REST: Passed 
# MAGIC - Evergage SFTP: Passed
# MAGIC - Confluent: Passed
# MAGIC - DBFS: Passed
# MAGIC 
# MAGIC **Test cases**
# MAGIC - count
# MAGIC - all data types(int, decimal, string, null)

# COMMAND ----------

# MAGIC %run "../src/fw_data_transformation" $job_id="transform_services_evergage" $predefined_connection ="predefined_connections_refactor_v1" $debug="True"

# COMMAND ----------

# MAGIC %run "../src/fw_data_services" $job_id="transform_services_evergage" $debug="True"

# COMMAND ----------

import unittest, logging, time
from py4j.protocol import *
from pyspark.sql.utils import *
from pyspark.sql.types import *

class TestServicesEvergage(unittest.TestCase): 
  def test_evergage_rest(self):
    ### data transformation ###
    config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/transform/"
    job_id = "transform_services_evergage"
    predefined_connection = "predefined_connections_refactor_v1"
    debug = True
  
    # read config
    job_input_config = TransformJobConfig(job_id, config_path).get_input_config_df()
    job_transform_config = TransformJobConfig(job_id, config_path).get_transform_config_df()
    job_output_config = TransformJobConfig(job_id, config_path).get_output_config_df()
    predefined_connection_config = PredefinedConnectionConfig(predefined_connection, predefined_config_path).get_config_df()

    # read input
    for c in job_input_config.collect():
      input_stream = ConnectionFactory(c, predefined_connection_config).create().get_body_df()
      input_stream.createOrReplaceTempView(c.input.name)

    # execute transformation
    for c in job_transform_config.collect():
      transformation = Transform(c, script_path)
      transformation.execute()
    
    # write output - pass global views to data services notebook
    job_output_config.createOrReplaceGlobalTempView(job_id + "__job_output_config")
    predefined_connection_config.createOrReplaceGlobalTempView(job_id + "__predefined_connection_config")
    for c in job_output_config.collect():
      view_name = c.output.name
      spark.table(view_name).createOrReplaceGlobalTempView(job_id + "__" + view_name)
    #dbutils.notebook.run("/Users/acpcloud2349@outlook.com/Data Platform/src/fw_data_services", 0, {"job_id": job_id, "debug": debug})
    
    ### data services ###
    global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
    # get params
    #job_id = dbutils.widgets.get("job_id")

    # get config from global view
    job_output_config = spark.table(global_temp_db + "." + job_id + "__job_output_config")
    predefined_connection_config = spark.table(global_temp_db + "." + job_id + "__predefined_connection_config")

    # write output
    for c in job_output_config.collect():
      output_stream = ViewToOutput(c, predefined_connection_config, job_id)
      output_stream.start()


    #spark.table("logging_events") \
    #.writeStream \
    #.format("memory") \
    #.queryName("got_input") \
    #.start()
    #
    #spark.table("targeted_marketing_events") \
    #.writeStream \
    #.format("memory") \
    #.queryName("got_transform") \
    #.start()
    #
    #get_df_from_eventhub("Endpoint=sb://dbfwpoc.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=EKyOWJIS0NAo0iwagcEy9ApLCkAHtDaYbrp/3e7uujQ=;EntityPath=desteventhub") \
    #.writeStream \
    #.format("memory") \
    #.queryName("got_output") \
    #.start()
    #
    #time.sleep(60)
    #
    #sendSample()
    #
    #time.sleep(60)
    #
    #want_input = 15
    #want_transform = 4
    #want_output = 4
    #
    #got_input = spark.table("got_input").count()
    #got_transform = spark.table("got_transform").count()
    #got_output = spark.table("got_output").count()
    #
    #self.assertEqual(got_input, want_input)
    #self.assertEqual(got_transform, want_transform)
    #self.assertEqual(got_output, want_output)
    #


# COMMAND ----------

test_cases = [(TestServicesEvergage)]

load_tests(test_cases)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.transform_services_evergage__scoring_events

# COMMAND ----------

display(spark.read.parquet("/mnt/lake/archive/transform_services_evergage/scoring_events").where(col('dp_data_dt')=='2020-12-24'))

# COMMAND ----------

display(spark.read.csv("/FileStore/tables/engage_users.csv", header=True).where(col("name0").startswith("dp_test")))

# COMMAND ----------

display(spark.table("dl_curated_cust_db.cust_info").where(col("rm_key")=="001400000000000000000019174143"))

# COMMAND ----------

#compare
df1 = spark.read.parquet("/mnt/lake/archive/transform_services_evergage/scoring_events").where(col('dp_data_dt')=='2020-12-24')
df2 = spark.read.csv("/FileStore/tables/engage_users.csv", header=True).where(col("name0").startswith("dp_test"))

# COMMAND ----------

df12 = df1.join(df2, df1.custId == df2.name0).where(df1.accountId == df2.accountId)#.where(df1.testScore == df2.testScore) #TBD Evergage bug
df1.count() == df12.count()

# COMMAND ----------

# MAGIC %md ### Evergage SFTP 

# COMMAND ----------

### data transformation ###
config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/transform/"
job_id = "transform_services_dynamic_pricing"
predefined_connection = "predefined_connections_refactor_v1"
debug = True

# read config
job_input_config = TransformJobConfig(job_id, config_path).get_input_config_df()
job_transform_config = TransformJobConfig(job_id, config_path).get_transform_config_df()
job_output_config = TransformJobConfig(job_id, config_path).get_output_config_df()
predefined_connection_config = PredefinedConnectionConfig(predefined_connection, predefined_config_path).get_config_df()

# read input
for c in job_input_config.collect():
  input_stream = ConnectionFactory(c, predefined_connection_config).create().get_body_df()
  input_stream.createOrReplaceTempView(c.input.name)

# execute transformation
for c in job_transform_config.collect():
  transformation = Transform(c, script_path)
  transformation.execute()

# write output - pass global views to data services notebook
job_output_config.createOrReplaceGlobalTempView(job_id + "__job_output_config")
predefined_connection_config.createOrReplaceGlobalTempView(job_id + "__predefined_connection_config")
for c in job_output_config.collect():
  view_name = c.output.name
  spark.table(view_name).createOrReplaceGlobalTempView(job_id + "__" + view_name)
#dbutils.notebook.run("/Users/acpcloud2349@outlook.com/Data Platform/src/fw_data_services", 0, {"job_id": job_id, "debug": debug})

### data services ###
global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
# get params
#job_id = dbutils.widgets.get("job_id")

# get config from global view
job_output_config = spark.table(global_temp_db + "." + job_id + "__job_output_config")
predefined_connection_config = spark.table(global_temp_db + "." + job_id + "__predefined_connection_config")

# write output
for c in job_output_config.collect():
  output_stream = ViewToOutput(c, predefined_connection_config, job_id)
  output_stream.start()

# COMMAND ----------

df1 = spark.read.csv("dbfs:/dataservices/evergage-sftp/transform_services_dynamic_pricing/cust_info_dynamic_pricing/csv", header = True)
display(df1.sort(col("userId")))

# COMMAND ----------

import pysftp

sftpHostname = 'sftp.us-4.evergage.com'
sftpUsername = 'AACE1796-CB8A-4134-B267-8B57F7EDDA6A'
sftpPassword = 'aHgxB8vn8TdAD2KrLuYsg_wVZtZq8W5k_JUPGyfKEcI'
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None 

with pysftp.Connection(sftpHostname, username=sftpUsername, password=sftpPassword, cnopts=cnopts) as sftp:
  sftp.get('/engage/processed/user-2020-12-24_16-51-21.csv', '/dbfs/tmp/user-2020-12-24_16-51-21.csv')

# COMMAND ----------

df2 = spark.read.csv("/tmp/user-2020-12-24_16-51-21.csv", header = True)
display(df2.sort(col("userId")))

# COMMAND ----------

df12 = df1.join(df2, df1.userId == df2.userId).where(
  (df1['attribute:accountId'] == df2['attribute:accountId']) &
  (df1['attribute:occupationCode'] == df2['attribute:occupationCode']) &
  (df1['attribute:RMCustID'] == df2['attribute:RMCustID']) &
  (df1['attribute:birthDate'] == df2['attribute:birthDate']) &
  (df1['attribute:age'] == df2['attribute:age']) &
  (df1['attribute:genderCode'] == df2['attribute:genderCode']) &
  (df1['attribute:customerIncome'] == df2['attribute:customerIncome']) &
  (df1['attribute:customerSegment'] == df2['attribute:customerSegment']) &
  (df1['attribute:isMcmc'] == df2['attribute:isMcmc'])
)
 
df1.count() == df12.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cust_info_dynamic_pricing

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from logging_events e

# COMMAND ----------

# MAGIC %md ### Confluent

# COMMAND ----------

# MAGIC %run "../src/fw_data_transformation" $job_id="transform_marketing_confluent" $predefined_connection ="predefined_connections_refactor_v1" $debug="True"

# COMMAND ----------

# MAGIC %run "../src/fw_data_services" $job_id="transform_marketing_confluent" $debug="True"

# COMMAND ----------

### data transformation ###
config_path = "wasbs://config@dbfwpoc.blob.core.windows.net/transform/"
job_id = "transform_marketing_confluent"
predefined_connection = "predefined_connections_refactor_v1"
debug = True

# read config
job_input_config = TransformJobConfig(job_id, config_path).get_input_config_df()
job_transform_config = TransformJobConfig(job_id, config_path).get_transform_config_df()
job_output_config = TransformJobConfig(job_id, config_path).get_output_config_df()
predefined_connection_config = PredefinedConnectionConfig(predefined_connection, predefined_config_path).get_config_df()

# read input
for c in job_input_config.collect():
  input_stream = ConnectionFactory(c, predefined_connection_config).create().get_body_df()
  input_stream.createOrReplaceTempView(c.input.name)

# execute transformation
for c in job_transform_config.collect():
  transformation = Transform(c, script_path)
  transformation.execute()

# write output - pass global views to data services notebook
job_output_config.createOrReplaceGlobalTempView(job_id + "__job_output_config")
predefined_connection_config.createOrReplaceGlobalTempView(job_id + "__predefined_connection_config")
for c in job_output_config.collect():
  view_name = c.output.name
  spark.table(view_name).createOrReplaceGlobalTempView(job_id + "__" + view_name)
#dbutils.notebook.run("/Users/acpcloud2349@outlook.com/Data Platform/src/fw_data_services", 0, {"job_id": job_id, "debug": debug})

### data services ###
global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
# get params
#job_id = dbutils.widgets.get("job_id")

# get config from global view
job_output_config = spark.table(global_temp_db + "." + job_id + "__job_output_config")
predefined_connection_config = spark.table(global_temp_db + "." + job_id + "__predefined_connection_config")

# write output
for c in job_output_config.collect():
  output_stream = ViewToOutput(c, predefined_connection_config, job_id)
  output_stream.start()

# COMMAND ----------

display(spark.table("targeted_marketing_events"))

# COMMAND ----------

test01 = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "pkc-l9wvm.ap-southeast-1.aws.confluent.cloud:9092")\
  .option("subscribe", "test01").option("startingOffsets", "latest")\
  .option("kafka.security.protocol","SASL_SSL")\
  .option("kafka.sasl.mechanism", "PLAIN")\
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"YYPGB4ZHMBOQDPCN\" password=\"FV9CweH7w+mcZxk8PLJaGYFba5HfV+xVCk1BHN52i0MTA/KOutICfLlufQTvj6S/\";") \
  .load()

display(test01.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp", "timestampType"))

# COMMAND ----------


