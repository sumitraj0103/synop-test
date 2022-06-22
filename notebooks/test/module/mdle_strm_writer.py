# Databricks notebook source
#%run "../../fw/cmmn/config/environment_config"

# COMMAND ----------

# # Example
# key_col = "id"
# schema = struct("amount","credit_number","data_dt","event_code", "test_number")
# confluentBootstrapServers = "pkc-l9wvm.ap-southeast-1.aws.confluent.cloud:9092"
# confluentTopicName = "test01"

# COMMAND ----------

def writeConfluent(sdf, key_col, val_col, secretScope, confluentBootstrapServers, confluentSecretsPrefix, confluentTopicName):
  confluentApiKey = dbutils.secrets.get(scope = secretScope, key = confluentSecretsPrefix + "-api-key")
  confluentSecret = dbutils.secrets.get(scope = secretScope, key = confluentSecretsPrefix + "-secret")
  
  writeQuery = (
    sdf.select(col(key_col).alias('key'), to_json(struct(sdf.columns)).alias('value'))
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", confluentBootstrapServers)
    .option("topic", confluentTopicName)
    .option("kafka.security.protocol","SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
    .save()
  )
