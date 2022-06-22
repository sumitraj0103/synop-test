# Databricks notebook source
# MAGIC %run "../fw/cmmn/config/environment_config"

# COMMAND ----------

# MAGIC %run "../fw/coll/fw_coll"

# COMMAND ----------

# MAGIC %run "../fw/cmmn/module/fw_cmmn_func"

# COMMAND ----------

# MAGIC %md ### unit test

# COMMAND ----------

import unittest, logging, time
from py4j.protocol import *
from pyspark.sql.utils import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType
from box import Box

class TestReadWriteADLS(unittest.TestCase):
  
  def test_fw_coll(self):
    # Arrange
    schema = StructType([
      StructField('amount', StringType(), True),
      StructField('credit_number', StringType(), True),
      StructField('data_dt', StringType(), True),
      StructField('event_code', StringType(), True),
      StructField('id', StringType(), True),
      StructField('test_number', StringType(), True),
      StructField('time_constant', StringType(), True),
      StructField('index', StringType(), True),
      StructField('acct_num', StringType(), True)
    ])
    sdf = spark.readStream.format('csv').option('maxFilesPerTrigger', 1).option("header", "true").schema(schema).load('/FileStore/data/*ampleDF.csv')
    d = {
      "raw": {
          "path": "raw/test_fw_coll"
      }
    }
    c = Box(**d)
    input_stream = sdf
    
    # Act
    sr = StreamToRaw(input_stream, c.raw.path)
    sr.write()
    
    time.sleep(15)
    
    # Assert
    got = spark.read.parquet(COLL_RAW_PATH + c.raw.path).count()
    want = 15
    self.assertEqual(got, want)
    
test_cases = ([TestReadWriteADLS])
test_results = []

def load_tests():
    runner = unittest.TextTestRunner(verbosity=2)
    for test_class in test_cases:
      print(test_class)
      suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
      test_results.append(runner.run(suite))
    
load_tests()

#if __name__ == '__main__':
#    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestExample)
#    test_result = TestRunner().run(test_suite)

# COMMAND ----------

# Stop streams

for s in spark.streams.active:
  if s.name in ['FW_STRM_COLL > raw/test_fw_coll']:
    s.stop()
    s.awaitTermination(1000)

# COMMAND ----------

#for r in test_results:
#  if(not r.wasSuccessful()):
#    dbutils.notebook.exit("failed")
#    
#dbutils.notebook.exit("success")

# COMMAND ----------

for t in test_results:
  result = "success" if(t.wasSuccessful()) else "failed"
  query = f"\
  insert into dpdev_mntr_db.test_log \
  select \
  current_date,\
  current_timestamp, \
  'TEST_FW_COLL' as test_type, \
  cast('{result}' as string) as test_sts, \
  '{FW_VERSION}' as fw_version \
  "
  spark.sql(query)

# COMMAND ----------


