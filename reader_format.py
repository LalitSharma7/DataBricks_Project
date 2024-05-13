# Databricks notebook source
class Datasource:
    """
    Abstract class
    """
    def __init__(self, path):
        self.path = path
    
    def get_data_frame(self):
        """
        Abstract methoda , function will be defined in sub classes
        """
        raise ValueError("Not Implemented")


class CSVDataSource(Datasource):
    def get_data_frame(self):
        return spark.read.format("CSV").option("header", "True").load(self.path)


class ParquetDataSource(Datasource):
    def get_data_frame(self):
        return spark.read.option("header", "True").load(self.path)
    

class DeltaDataSource(Datasource):
    def get_data_frame(self):
        table_name = self.path
        return spark.read.table(table_name)
    


def get_data_source(data_type, file_path):

    if data_type == "csv":
        return CSVDataSource(file_path)
    elif data_type== "parquet":
        return ParquetDataSource(file_path)
    elif data_type == "delta":
        return DeltaDataSource(file_path)
    else:
        raise ValueError(f"Not implemented for this data_type: {data_type}")