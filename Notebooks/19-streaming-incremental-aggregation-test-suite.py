# Databricks notebook source
# MAGIC %run ./18-streaming-incremental-aggregation

# COMMAND ----------

class AggregationTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        spark.sql("drop table if exists invoices_bz")
        spark.sql("drop table if exists customer_rewards")
        dbutils.fs.rm("/user/hive/warehouse/invoices_bz", True)        
        dbutils.fs.rm("/user/hive/warehouse/customer_rewards", True)
        spark.sql(f"CREATE TABLE customer_rewards(CustomerCardNo STRING, TotalAmount DOUBLE, TotalPoints DOUBLE)")

        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/invoices_bz", True)
        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/customer_rewards", True)

        dbutils.fs.rm(f"{self.base_data_dir}/data/invoices", True)
        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/invoices")
        print("Done")

    def ingestData(self, itr):
        print(f"\tStarting Ingestion...", end='')
        dbutils.fs.cp(f"{self.base_data_dir}/datasets/invoices/invoices_{itr}.json", f"{self.base_data_dir}/data/invoices/")
        print("Done")

    def assertBronze(self, expected_count):
        print(f"\tStarting Bronze validation...", end='')
        actual_count = spark.sql("select count(*) from invoices_bz").collect()[0][0]
        assert expected_count == actual_count, f"Test failed! actual count is {actual_count}"
        print("Done")

    def assertGold(self, expected_value):
        print(f"\tStarting Gold validation...", end='')
        actual_value = spark.sql("select TotalAmount from customer_rewards where CustomerCardNo = '2262471989'").collect()[0][0]
        assert expected_value == actual_value, f"Test failed! actual value is {actual_value}"
        print("Done")

    def waitForMicroBatch(self, sleep=60):
        import time
        print(f"\tWaiting for {sleep} seconds...", end='')
        time.sleep(sleep)
        print("Done.")    

    def runTests(self):
        self.cleanTests()
        spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
                       "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        bzStream = Bronze()
        bzQuery = bzStream.process()
        gdStream = Gold()
        gdQuery = gdStream.process()       

        print("\nTesting first iteration of invoice stream...") 
        self.ingestData(1)
        self.waitForMicroBatch()        
        self.assertBronze(501)
        self.assertGold(36859)
        print("Validation passed.\n")

        print("\nTesting second iteration of invoice stream...") 
        self.ingestData(2)
        self.waitForMicroBatch()        
        self.assertBronze(501+500)
        self.assertGold(36859+20740)
        print("Validation passed.\n")

        print("\nTesting third iteration of invoice stream...") 
        self.ingestData(3)
        self.waitForMicroBatch()        
        self.assertBronze(501+500+590)
        self.assertGold(36859+20740+31959)
        print("Validation passed.\n")

        bzQuery.stop()
        gdQuery.stop()

# COMMAND ----------

aTS = AggregationTestSuite()
aTS.runTests()	
