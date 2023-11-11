# Databricks notebook source
class SlidingAggregate():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def getSchema(self):
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        return StructType([
                    StructField("CreatedTime", StringType()),
                    StructField("Reading", DoubleType())
                ])
        
    def readBronze(self):
        return spark.readStream.table("kafka_bz")
    
    def getSensorData(self, kafka_df):
        from pyspark.sql.functions import from_json, expr
        return (kafka_df.select(kafka_df.key.cast("string").alias("SensorID"),
                                from_json(kafka_df.value.cast("string"), self.getSchema()).alias("value"))
                        .select("SensorID", "value.*")
                        .withColumn("CreatedTime", expr("to_timestamp(CreatedTime, 'yyyy-MM-dd HH:mm:ss')"))
                )
        
    def getAggregate(self, sensor_df):
        from pyspark.sql.functions import window, max
        return (sensor_df.withWatermark("CreatedTime", "30 minutes")
                        .groupBy(sensor_df.SensorID,
                                 window(sensor_df.CreatedTime, "15 minutes", "5 minute"))
                        .agg(max("Reading").alias("MaxReading"))
                        .select("SensorID", "window.start", "window.end", "MaxReading")
                )
        
    def saveResults(self, results_df):
        print(f"\nStarting Silver Stream...", end='')
        return (results_df.writeStream
                    .queryName("sensor-query")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/sensor_summary")
                    .outputMode("complete")
                    .toTable("sensor_summary")
                )
        print("Done")

    def process(self):
        kafka_df = self.readBronze()
        sensor_df = self.getSensorData(kafka_df)
        results_df = self.getAggregate(sensor_df)        
        sQuery = self.saveResults(results_df)
        return sQuery
