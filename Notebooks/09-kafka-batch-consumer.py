# Databricks notebook source
# MAGIC %md
# MAGIC ####Install below package in your cluster
# MAGIC org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

# COMMAND ----------

BOOTSTRAP_SERVER = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092"
JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
CLUSTER_API_KEY = "TQ7F373OG6J4TT4G"
CLUSTER_API_SECRET = "ftb0mWOYcCT6oAG9GSIjyUxHYuJmbUPDi0G0c5ymt0t6FKJ1J1IVDbDRVqKoG5H4"

# COMMAND ----------

df = ( spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", f"{JAAS_MODULE} required username='{CLUSTER_API_KEY}' password='{CLUSTER_API_SECRET}';")
            .option("subscribe", "invoices")
            .load()
        )

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 <a href="https://www.scholarnest.com/">ScholarNest Technologies Pvt. Ltd. </a>All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact-us/">Contact Us</a>
