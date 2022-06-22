# Databricks notebook source
from pyspark.sql import Window

# COMMAND ----------

test_notebooks = ['test_fw_coll']

# COMMAND ----------

for nb in test_notebooks:
  dbutils.notebook.run(nb, 0)

# COMMAND ----------

df = spark.table("dpdev_mntr_db.test_log")
w = Window.partitionBy('test_type').orderBy(desc('log_ts'))
df = df.withColumn('rank',dense_rank().over(w))

df = df.filter(df.rank == 1).drop(df.rank)
display(df)

# COMMAND ----------


