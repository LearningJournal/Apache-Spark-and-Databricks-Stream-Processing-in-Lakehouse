# Databricks notebook source
# MAGIC %run ./14-kafka-producer

# COMMAND ----------

class KafkaProducerTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"
     
    def cleanTests(self):
        print(f"Starting Cleanup...", end='')        
        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/kafka_producer", True)
        dbutils.fs.rm(f"{self.base_data_dir}/data/invoices", True)
        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/invoices")
        print("Done")

    def ingestData(self, itr):
        print(f"\tStarting Ingestion...", end='')
        dbutils.fs.cp(f"{self.base_data_dir}/datasets/invoices/invoices_{itr}.json", f"{self.base_data_dir}/data/invoices/")
        print("Done")

    def assertKafka(self, start_time, expected_count):
        pd = KafkaProducer()
        print(f"\tStarting validation...", end='')
        actual_count = ( spark.read
                            .format("kafka")
                            .option("kafka.bootstrap.servers", pd.BOOTSTRAP_SERVER)
                            .option("kafka.security.protocol", "SASL_SSL")
                            .option("kafka.sasl.mechanism", "PLAIN")
                            .option("kafka.sasl.jaas.config", f"{pd.JAAS_MODULE} required username='{pd.CLUSTER_API_KEY}' password='{pd.CLUSTER_API_SECRET}';")
                            .option("subscribe", "invoices")
                            .option("startingTimestamp", start_time)
                            .option("startingOffsetsByTimestampStrategy", "latest")
                            .load()
                            .count()
                        )
        assert expected_count == actual_count, f"Test failed! actual topic count is {actual_count}"
        print("Done")

    def waitForMicroBatch(self, sleep=10):
        import time
        print(f"\tWaiting for {sleep} seconds...", end='')
        time.sleep(sleep)
        print("Done.") 

    def runTests(self):
        import time
        start_time = round(time.time() * 1000)

        self.cleanTests()
        kp = KafkaProducer()
        kpStream = kp.process("StoreID == 'STR7188'")

        print("Testing first iteration...")        
        self.ingestData(1)
        self.waitForMicroBatch()
        self.assertKafka(start_time, 53)
        print("Validation passed.\n")

        print("Testing second iteration...")
        self.ingestData(2)
        self.waitForMicroBatch()
        self.assertKafka(start_time, 53+11)
        print("Validation passed.\n") 

        print("Testing third iteration...")
        self.ingestData(3)
        self.waitForMicroBatch()
        self.assertKafka(start_time, 53+11+25)
        print("Validation passed.\n")

        kpStream.stop()


# COMMAND ----------

kpTS = KafkaProducerTestSuite()
kpTS.runTests()	
