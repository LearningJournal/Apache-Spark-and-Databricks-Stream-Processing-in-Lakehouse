# Databricks notebook source
class Bronze():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

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

    def readInvoices(self):
        return (spark.readStream
                    .format("json")
                    .schema(self.getSchema())
                    #.option("cleanSource", "delete")
                    .option("cleanSource", "archive")
                    .option("sourceArchiveDir", f"{self.base_data_dir}/data/invoices_archive")
                    .load(f"{self.base_data_dir}/data/invoices")
                )  

    def process(self):
        print(f"\nStarting Bronze Stream...", end='')
        invoicesDF = self.readInvoices()
        sQuery =  ( invoicesDF.writeStream
                            .queryName("bronze-ingestion")
                            .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/invoices_bz")
                            .outputMode("append")
                            .toTable("invoices_bz")           
                    ) 
        print("Done")
        return sQuery   
       

# COMMAND ----------

class Silver():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def readInvoices(self):
        return ( spark.readStream
                    .table("invoices_bz")
                )

    def explodeInvoices(self, invoiceDF):
        return ( invoiceDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                                      "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
                                      "DeliveryAddress.State","DeliveryAddress.PinCode", 
                                      "explode(InvoiceLineItems) as LineItem")
                                    )  
           
    def flattenInvoices(self, explodedDF):
        from pyspark.sql.functions import expr
        return( explodedDF.withColumn("ItemCode", expr("LineItem.ItemCode"))
                        .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
                        .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
                        .withColumn("ItemQty", expr("LineItem.ItemQty"))
                        .withColumn("TotalValue", expr("LineItem.TotalValue"))
                        .drop("LineItem")
                )
        
    def appendInvoices(self, flattenedDF):
        return (flattenedDF.writeStream
                    .queryName("silver-processing")
                    .format("delta")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/invoice_line_items")
                    .outputMode("append")
                    .toTable("invoice_line_items")
        )

    def process(self):
           print(f"\nStarting Silver Stream...", end='')
           invoicesDF = self.readInvoices()
           explodedDF = self.explodeInvoices(invoicesDF)
           resultDF = self.flattenInvoices(explodedDF)
           sQuery = self.appendInvoices(resultDF)
           print("Done\n")
           return sQuery  

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 <a href="https://www.scholarnest.com/">ScholarNest Technologies Pvt. Ltd. </a>All rights reserved.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact-us/">Contact Us</a>
