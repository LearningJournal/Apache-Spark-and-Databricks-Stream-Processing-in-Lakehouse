# Databricks notebook source
# MAGIC %run ./07-medallion-approach

# COMMAND ----------

class medallionApproachTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        spark.sql("drop table if exists invoices_bz")
        spark.sql("drop table if exists invoice_line_items")
        dbutils.fs.rm("/user/hive/warehouse/invoices_bz", True)
        dbutils.fs.rm("/user/hive/warehouse/invoice_line_items", True)

        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/invoices_bz", True)
        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/invoice_line_items", True)

        dbutils.fs.rm(f"{self.base_data_dir}/data/invoices_archive", True)
        dbutils.fs.rm(f"{self.base_data_dir}/data/invoices", True)
        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/invoices")
        print("Done")

    def ingestData(self, itr):
        print(f"\tStarting Ingestion...", end='')
        dbutils.fs.cp(f"{self.base_data_dir}/datasets/invoices/invoices_{itr}.json", f"{self.base_data_dir}/data/invoices/")
        print("Done")

    def assertResult(self, expected_count):
        print(f"\tStarting validation...", end='')
        actual_count = spark.sql("select count(*) from invoice_line_items").collect()[0][0]
        assert expected_count == actual_count, f"Test failed! actual count is {actual_count}"
        print("Done")

    def waitForMicroBatch(self, sleep=30):
        import time
        print(f"\tWaiting for {sleep} seconds...", end='')
        time.sleep(sleep)
        print("Done.")    

    def runTests(self):
        self.cleanTests()
        bzStream = Bronze()
        bzQuery = bzStream.process()

        slStream = Silver()
        slQuery = slStream.process()

        print("\nTesting first iteration of invoice stream...") 
        self.ingestData(1)
        self.waitForMicroBatch()        
        self.assertResult(1249)
        print("Validation passed.\n")

        print("Testing second iteration of invoice stream...") 
        self.ingestData(2)
        self.waitForMicroBatch()
        self.assertResult(2506)
        print("Validation passed.\n") 

        print("Testing third iteration of invoice stream...") 
        self.ingestData(3)
        self.waitForMicroBatch()
        self.assertResult(3990)
        print("Validation passed.\n")

        bzQuery.stop()
        slQuery.stop()

        print("Validating Archive...", end="") 
        archives_expected = ["invoices_1.json", "invoices_2.json"]
        for f in dbutils.fs.ls(f"{self.base_data_dir}/data/invoices_archive/{self.base_data_dir}/data/invoices"):
            assert f.name in archives_expected, f"Archive Validation failed for {f.name}"
        print("Done")

# COMMAND ----------

maTS = medallionApproachTestSuite()
maTS.runTests()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 <a href="https://www.scholarnest.com/">ScholarNest Technologies Pvt. Ltd. </a>All rights reserved.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact-us/">Contact Us</a>
