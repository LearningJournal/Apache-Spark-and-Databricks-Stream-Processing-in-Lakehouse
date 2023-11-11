# Databricks notebook source
# MAGIC %run ./14-multi-query-kafka-sink

# COMMAND ----------

class kafkaToBronzeTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"
     
    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        spark.sql("drop table if exists invoices_bz")
        dbutils.fs.rm("/user/hive/warehouse/invoices_bz", True)
        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/invoices_bz", True)
        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/notification_feed", True)
        print("Done")

    def assertBronze(self, expected_count):
        print(f"\tStarting validation...", end='')
        actual_count = spark.sql("select count(*) from invoices_bz").collect()[0][0]
        assert expected_count == actual_count, f"Test failed! actual table count is {actual_count}"
        print("Done")

    def assertNotifications(self, start_time, expected_count):
        bz = Bronze()
        print(f"\tStarting validation...", end='')
        actual_count = ( spark.read
                            .format("kafka")
                            .option("kafka.bootstrap.servers", bz.BOOTSTRAP_SERVER)
                            .option("kafka.security.protocol", "SASL_SSL")
                            .option("kafka.sasl.mechanism", "PLAIN")
                            .option("kafka.sasl.jaas.config", f"{bz.JAAS_MODULE} required username='{bz.CLUSTER_API_KEY}' password='{bz.CLUSTER_API_SECRET}';")
                            .option("subscribe", "notifications")
                            .option("startingTimestamp", start_time)
                            .load()
                            .count()
                        )
        assert expected_count == actual_count, f"Test failed! actual topic count is {actual_count}"
        print("Done")

    def waitForMicroBatch(self, sleep=30):
        import time
        print(f"\tWaiting for {sleep} seconds...", end='')
        time.sleep(sleep)
        print("Done.") 

    def stopStreamingQueries(self):
        for q in spark.streams.active:
            if q.name in ["bronze-ingestion", "notification-feed"]:
                q.stop()

    def runTests(self):
        import time
        self.cleanTests()
        bzStream = Bronze()
        value_schema = bzStream.getSchema()
        spark.sql(f"CREATE TABLE invoices_bz (key STRING, value STRUCT<{value_schema}>, topic STRING, timestamp TIMESTAMP)")

        print("Testing Scenario - Start from beginneing on a new checkpoint...")
        current_time = round(time.time() * 1000)
        bzStream.process()
        self.waitForMicroBatch() 
        self.stopStreamingQueries()
        self.assertBronze(30)
        self.assertNotifications(current_time, 30)
        print("Validation passed.\n")

        print("Testing Scenarion - Restart from where it stopped on the same checkpoint...")
        bzStream.process()
        self.waitForMicroBatch()
        self.stopStreamingQueries()
        self.assertBronze(30)
        self.assertNotifications(current_time, 30)
        print("Validation passed.\n") 

        print("Testing Scenario - Start from 1697945539000 on a new checkpoint...") 
        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/invoices_bz", True)
        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/notification_feed", True)
        bzStream.process(1697945539000)
        self.waitForMicroBatch()
        self.stopStreamingQueries()
        self.assertBronze(30)
        self.assertNotifications(current_time, 40)
        print("Validation passed.\n")


# COMMAND ----------

ts = kafkaToBronzeTestSuite()
ts.runTests()
