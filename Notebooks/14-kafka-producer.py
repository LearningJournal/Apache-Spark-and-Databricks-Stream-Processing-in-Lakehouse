# Databricks notebook source
class KafkaProducer():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"
        self.BOOTSTRAP_SERVER = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092"
        self.JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.CLUSTER_API_KEY = "TQ7F373OG6J4TT4G"
        self.CLUSTER_API_SECRET = "ftb0mWOYcCT6oAG9GSIjyUxHYuJmbUPDi0G0c5ymt0t6FKJ1J1IVDbDRVqKoG5H4"

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

    def readInvoices(self, condition):
        from pyspark.sql.functions import expr
        return (spark.readStream
                    .format("json")
                    .schema(self.getSchema())
                    .load(f"{self.base_data_dir}/data/invoices")
                    .where(condition)
                )
        
    def getKafkaMessage(self, df, key):
        return df.selectExpr(f"{key} as key", "to_json(struct(*)) as value")
    
    def sendToKafka(self, kafka_df):
        return ( kafka_df.writeStream
                    .queryName("kafka-producer")
                    .format("kafka")
                    .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER)
                    .option("kafka.security.protocol", "SASL_SSL")
                    .option("kafka.sasl.mechanism", "PLAIN")
                    .option("kafka.sasl.jaas.config", f"{self.JAAS_MODULE} required username='{self.CLUSTER_API_KEY}' password='{self.CLUSTER_API_SECRET}';")
                    .option("topic", "invoices")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/kafka_producer")
                    .outputMode("append")
                    .start()
            )       

    def process(self, condition):
           print(f"Starting Kafka Producer Stream...", end='')
           invoices_df = self.readInvoices(condition)
           kafka_df = self.getKafkaMessage(invoices_df, "StoreID")
           sQuery = self.sendToKafka(kafka_df)
           print("Done\n")
           return sQuery     
