# Databricks notebook source
class TradeSummary():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def getSchema(self):
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        return StructType([
                StructField("CreatedTime", StringType()),
                StructField("Type", StringType()),
                StructField("Amount", DoubleType()),
                StructField("BrokerCode", StringType())
            ])
        
    def readBronze(self):
        return spark.readStream.table("kafka_bz")

    def getTrade(self, kafka_df):
        from pyspark.sql.functions import from_json, expr
        return ( kafka_df.select(from_json(kafka_df.value, self.getSchema()).alias("value"))
                         .select("value.*")
                         .withColumn("CreatedTime", expr("to_timestamp(CreatedTime, 'yyyy-MM-dd HH:mm:ss')"))
                         .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end"))
                         .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))
                )

    def getAggregate(self, trades_df):
        from pyspark.sql.functions import window, sum
        return (trades_df.withWatermark("CreatedTime", "30 minutes")
                        .groupBy(window(trades_df.CreatedTime, "15 minutes"))
                        .agg(sum("Buy").alias("TotalBuy"),
                             sum("Sell").alias("TotalSell"))
                        .select("window.start", "window.end", "TotalBuy", "TotalSell")
                )

    def saveResults(self, results_df):
        print(f"\nStarting Silver Stream...", end='')
        return (results_df.writeStream
                    .queryName("trade-summary")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/trade_summary")
                    .outputMode("complete")
                    .toTable("trade_summary")
                )
        print("Done")

    def process(self):
        kafka_df = self.readBronze()
        trades_df = self.getTrade(kafka_df)
        results_df = self.getAggregate(trades_df)        
        sQuery = self.saveResults(results_df)
        return sQuery

