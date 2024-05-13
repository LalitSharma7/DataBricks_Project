# Databricks notebook source
# MAGIC %run "./reader_format"

# COMMAND ----------

class Extractor:
    def __init__(self):
        pass

    def extractor(self):
        pass


class AirpodsAfterIphoneExtractor(Extractor):
    """
    Steps for extracting data
    """
    def extractor(self):
        # Transaction df in csv format
        transactionDF = get_data_source(
            data_type="csv",
            file_path="dbfs:/FileStore/tables/Transaction_Updated.csv"
        ).get_data_frame()

        transactionDF.orderBy("customer_id", "transaction_date")

        # customer data in delta format
        customerDF = get_data_source(
            data_type="delta",
            file_path="default.customer_delta_table"
        ).get_data_frame()

        # customerDF.show()

        inputDFs = {
            "transactionDF": transactionDF,
            "customerDF": customerDF
        }

        return inputDFs
