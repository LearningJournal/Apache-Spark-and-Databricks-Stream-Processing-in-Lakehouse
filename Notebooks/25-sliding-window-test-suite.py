# Databricks notebook source
# MAGIC %run ./24-sliding-window

# COMMAND ----------

class SensorSummaryTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        spark.sql("drop table if exists kafka_bz")
        spark.sql("drop table if exists sensor_summary")
        dbutils.fs.rm("/user/hive/warehouse/kafka_bz", True)
        dbutils.fs.rm("/user/hive/warehouse/sensor_summary", True)
        spark.sql(f"CREATE TABLE kafka_bz(key STRING, value STRING)")

        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/sensor_summary", True)
        print("Done")

    def waitForMicroBatch(self, sleep=60):
        import time
        print(f"\tWaiting for {sleep} seconds...", end='')
        time.sleep(sleep)
        print("Done.")    

    def assertSensorSummary(self, id, start, end, expected_max):
        print(f"\tStarting Sensor Summary validation...", end='')
        actual_result = spark.table("sensor_summary").collect()
        expected_result = (spark.read.format("csv")
                                .option("header", "true")
                                .load(f"{self.base_data_dir}/datasets/results/sliding_window_result.csv")
                                .collect()
                            )
        assert expected_result == actual_result, f"Test failed! actual max is {actual_result}"
        print("Done")

    def runTests(self):
        self.cleanTests()

        stream = SlidingAggregate()
        sQuery = stream.process()       

        print("\nTesting all events...") 
        spark.sql("""INSERT INTO kafka_bz VALUES
                  ('SET41', '{"CreatedTime": "2019-02-05 09:54:00","Reading": 36.2}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 09:59:00","Reading": 36.5}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 10:04:00","Reading": 36.8}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 10:09:00","Reading": 36.2}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 10:14:00","Reading": 36.5}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 10:19:00","Reading": 36.3}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 10:24:00","Reading": 37.7}'),
                  ('SET41', '{"CreatedTime": "2019-02-05 10:29:00","Reading": 37.2}')
            """)
        self.waitForMicroBatch()
        print("Validation passed.\n")        

        sQuery.stop()    
    

# COMMAND ----------

ts = SensorSummaryTestSuite()
ts.runTests()
