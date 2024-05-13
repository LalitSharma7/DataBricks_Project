# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains


class Transform():
    def __init__(self):
        pass

    def transform(self):
        pass

class AirPodsAfterIphoneTransformer:
    def transform(self, inputDFs):
        """
        Customer who have bought airpods after buying iphone
        """
        transactionDF = inputDFs.get("transactionDF")

        print("transactionDF in transform")

        #transactionDF.show()
        window = Window.partitionBy("customer_id").orderBy("transaction_date")
        transformedDF = transactionDF.withColumn(
            "next_product_name", lead("product_name").over(window)
        )

        filteredDF = transformedDF.filter(
            (col("product_name")=="iPhone") & (col("next_product_name")=="AirPods")
        )

        customerDF = inputDFs.get("customerDF")

        joinDF = customerDF.join(broadcast(filteredDF), "customer_id")
        
        print("Customer name who bought Airpods after iphone")
    
        display(joinDF.select("customer_id", "customer_name", "location"))
        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )

class OnlyAirpodsandIphoneTransformer:
    def transform(self, inputDFs):
        """
        Customer who have bought only iphone and airpods
        """
        transactionDF = inputDFs.get("transactionDF")
        

        groupedDF = transactionDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )

        print("Grouped DF")
        #groupedDF.show()

        filteredDF = groupedDF.filter(
            (array_contains(col("products"), "iPhone")) &
            (array_contains(col("products"), "AirPods")) & 
            (size(col("products")) == 2)
        )
        
    
        customerInputDF = inputDFs.get("customerDF")
        joinDF =  customerInputDF.join(
           broadcast(filteredDF),
            "customer_id"
        )

        display(joinDF.show())

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )

