# Databricks notebook source
# MAGIC %md
# MAGIC ####Install following package
# MAGIC org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

# COMMAND ----------

class Bronze():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"
        self.BOOTSTRAP_SERVER = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092"
        self.JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.CLUSTER_API_KEY = "TQ7F373OG6J4TT4G"
        self.CLUSTER_API_SECRET = "ftb0mWOYcCT6oAG9GSIjyUxHYuJmbUPDi0G0c5ymt0t6FKJ1J1IVDbDRVqKoG5H4"

    def ingestFromKafka(self, startingTime = 1):
        return ( spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER)
                    .option("kafka.security.protocol", "SASL_SSL")
                    .option("kafka.sasl.mechanism", "PLAIN")
                    .option("kafka.sasl.jaas.config", f"{self.JAAS_MODULE} required username='{self.CLUSTER_API_KEY}' password='{self.CLUSTER_API_SECRET}';")
                    .option("maxOffsetsPerTrigger", 30)
                    .option("subscribe", "invoices")
                    .option("startingTimestamp", startingTime)
                    .load()
                )
        
    def getSchema(self):
        return """InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string,
                CustomerType string, CustomerCardNo string, TotalAmount double, NumberOfItems bigint, 
                PaymentMethod string, TaxableAmount double, CGST double, SGST double, CESS double, 
                DeliveryType string,
                DeliveryAddress struct<AddressLine string, City string, ContactNumber string, PinCode string, 
                State string>,
                InvoiceLineItems array<struct<ItemCode string, ItemDescription string, 
                    ItemPrice double, ItemQty bigint, TotalValue double>>
            """
        
    def getInvoices(self, kafka_df):
        from pyspark.sql.functions import from_json 
        return ( kafka_df.select(kafka_df.key.cast("string").alias("key"),
                                 from_json(kafka_df.value.cast("string"), self.getSchema()).alias("value"),
                                 "topic", "timestamp")
        )

    def upsert(self, invoices_df, batch_id):        
        invoices_df.createOrReplaceTempView("invoices_df_temp_view")
        merge_statement = """MERGE INTO invoices_bz s
                USING invoices_df_temp_view t
                ON s.value == t.value AND s.timestamp == t.timestamp
                WHEN MATCHED THEN
                UPDATE SET *
                WHEN NOT MATCHED THEN
                INSERT *
            """
        invoices_df._jdf.sparkSession().sql(merge_statement)
         
    def process(self, startingTime = 1):
        print(f"Starting Bronze Stream...", end='')
        kafka_df = self.ingestFromKafka(startingTime)
        invoices_df =  self.getInvoices(kafka_df)
        sQuery = ( invoices_df.writeStream
                            .queryName("bronze-ingestion")
                            .foreachBatch(self.upsert)
                            .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/invoices_bz")
                            .outputMode("append")
                            .start()
                )
        print("Done")
        return sQuery                 

