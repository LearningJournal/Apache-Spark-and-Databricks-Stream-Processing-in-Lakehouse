# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Upserter:
    def __init__(self, merge_query, temp_view_name):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name 
        
    def upsert(self, df_micro_batch, batch_id):
        df_micro_batch.createOrReplaceTempView(self.temp_view_name)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# COMMAND ----------

class Gold():
    def __init__(self, env):
        self.Conf = Config() 
        self.test_data_dir = self.Conf.base_dir_data + "/test_data"
        self.checkpoint_base = self.Conf.base_dir_checkpoint + "/checkpoints"
        self.catalog = env
        self.db_name = self.Conf.db_name
        self.maxFilesPerTrigger = self.Conf.maxFilesPerTrigger
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        
    def upsert_workout_bpm_summary(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        
        #Idempotent - Once a workout session is complete, It doesn't change. So insert only the new records
        query = f"""
        MERGE INTO {self.catalog}.{self.db_name}.workout_bpm_summary a
        USING workout_bpm_summary_delta b
        ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id
        WHEN NOT MATCHED THEN INSERT *
        """
        
        data_upserter=Upserter(query, "workout_bpm_summary_delta")
        
        df_users = spark.read.table(f"{self.catalog}.{self.db_name}.user_bins")
        
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         #.option("ignoreDeletes", True)
                         #.option("withEventTimeOrder", "true")
                         #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                         .table(f"{self.catalog}.{self.db_name}.workout_bpm")
                         .withWatermark("end_time", "30 seconds")
                         .groupBy("user_id", "workout_id", "session_id", "end_time")
                         .agg(F.min("heartrate").alias("min_bpm"), F.mean("heartrate").alias("avg_bpm"), 
                              F.max("heartrate").alias("max_bpm"), F.count("heartrate").alias("num_recordings"))                         
                         .join(df_users, ["user_id"])
                         .select("workout_id", "session_id", "user_id", "age", "gender", "city", "state", "min_bpm", "avg_bpm", "max_bpm", "num_recordings")
                     )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("append")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/workout_bpm_summary")
                                 .queryName("workout_bpm_summary_upsert_stream")
                        )
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "gold_p1")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
    
    
    def upsert(self, once=True, processing_time="5 seconds"):
        import time
        start = int(time.time())
        print(f"\nExecuting gold layer upsert ...")
        self.upsert_workout_bpm_summary(once, processing_time)
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
        print(f"Completed gold layer upsert {int(time.time()) - start} seconds")
        
        
    def assert_count(self, table_name, expected_count, filter="true"):
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}" 
        print(f"Found {actual_count:,} / Expected {expected_count:,} records where {filter}: Success") 
        
    def assert_rows(self, location, table_name, sets):
        print(f"Validating records in {table_name}...", end='')
        expected_rows = spark.read.format("parquet").load(f"{self.test_data_dir}/{location}_{sets}.parquet").collect()
        actual_rows = spark.table(table_name).collect()
        assert expected_rows == actual_rows, f"Expected data mismatches with the actual data in {table_name}"
        print(f"Expected data matches with the actual data in {table_name}: Success")
        
        
    def validate(self, sets):
        import time
        start = int(time.time())
        print(f"\nValidating gold layer records..." )       
        self.assert_rows("7-gym_summary", "gym_summary", sets)       
        if sets>1:
            self.assert_count("workout_bpm_summary", 2)
        print(f"Gold layer validation completed in {int(time.time()) - start} seconds")        
