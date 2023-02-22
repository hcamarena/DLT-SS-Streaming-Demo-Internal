# Databricks notebook source
EH_NAMESPACE = "oneenv-eventhub"
## THIS TOPIC IS TO BE SHARED AND USED FOR DEMO
EH_KAFKA_TOPIC = "one-env-windturbine"
connSharedAccessKey = dbutils.secrets.get("streaming-demo","kafka_connection_key")

# Get Databricks secret value 
connSharedAccessKeyName = "RootManageSharedAccessKey"

EH_BOOTSTRAP_SERVERS = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
EH_SASL = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={connSharedAccessKeyName};SharedAccessKey={connSharedAccessKey};EntityPath={EH_KAFKA_TOPIC}\";"

# COMMAND ----------

# Full username, e.g. "<first>.<last>@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")

# DBFS directory for this project, we will store the Kafka checkpoint in there
project_dir = f"/home/{username}/kafka_wind_turbine_dlt_streaming"

checkpoint_location = f"{project_dir}/kafka_checkpoint"

# COMMAND ----------

# DBTITLE 1,Read Turbine Dataset (200k records)
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import random, string, uuid

uuidUdf= udf(lambda : uuid.uuid4().hex,StringType())

input_path = "/demos/manufacturing/iot_turbine/incoming-data-json"
input_schema = spark.read.json(input_path).schema

input_stream = (spark
  .readStream
  .schema(input_schema)
  .json(input_path)
  .withColumn("processingTime", lit(datetime.now().timestamp()).cast("timestamp"))
  .withColumn("eventId", uuidUdf()))

# COMMAND ----------

# DBTITLE 1,Publish To Kafka Topic
# Clear checkpoint location
dbutils.fs.rm(checkpoint_location, True)

# For the sake of an example, we will write to the Kafka servers using SSL/TLS encryption
# Hence, we have to set the kafka.security.protocol property to "SSL"
(input_stream
   .select(col("eventId").alias("key"), to_json(struct(col('TORQUE'), col('TIMESTAMP'),col('SPEED'),col('ID'), col('AN3'), col('AN4'), col('AN5'),col('AN6'),col('AN7'),col('AN8'),col('AN9'),col('AN10'),col('processingTime'))).alias("value"))
   .writeStream
   .trigger(once=True) 
   .format("kafka")
   .option("topic", EH_KAFKA_TOPIC) 
   .option("kafka.bootstrap.servers", EH_BOOTSTRAP_SERVERS) 
   .option("kafka.sasl.mechanism", "PLAIN") 
   .option("kafka.security.protocol", "SASL_SSL") 
   .option("kafka.sasl.jaas.config", EH_SASL) 
   .option("startingOffsets","earliest") 
   .option("checkpointLocation", checkpoint_location )
   .start()
)

# COMMAND ----------

# DBTITLE 1,Consume & Test Kafka Source 
# startingOffsets = "earliest" 

# # In contrast to the Kafka write in the previous cell, when we read from Kafka we use the unencrypted endpoints.
# # Thus, we omit the kafka.security.protocol property
# kafka = (spark.readStream
#   .format("kafka")
#   .option("subscribe", EH_KAFKA_TOPIC) 
#   .option("kafka.bootstrap.servers", EH_BOOTSTRAP_SERVERS) 
#   .option("kafka.sasl.mechanism", "PLAIN") 
#   .option("kafka.security.protocol", "SASL_SSL") 
#   .option("kafka.sasl.jaas.config", EH_SASL) 
#   .option("startingOffsets", startingOffsets) 
#   .load())

# read_stream = kafka.select(col("key").cast("string").alias("eventId"), from_json(col("value").cast("string"), input_schema).alias("json"))

# display(read_stream)
