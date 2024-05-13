# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./transform"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

class FirstWorkflow:
    """
    ETL pipleline to determine customers who bought airpods after iphone
    """

    def __init__(self):
        pass

    def runner(self):
        
        # Step01 :- Extracting data 
        inputDFs = AirpodsAfterIphoneExtractor().extractor()

        # Step02 :- Transforming data
        firstTransformDF = AirPodsAfterIphoneTransformer().transform(inputDFs)

        # Step03 :- Loading data
        AirPodsAfterIphoneLoader(firstTransformDF).sink()


# COMMAND ----------

class SecondWorkflow:
    """
    ETL pipleline to determine customers who bought only iphone and airpods
    """

    def __init__(self):
        pass

    def runner(self):
        
        # Step01 :- Extracting data 
        inputDFs = AirpodsAfterIphoneExtractor().extractor()

        # Step02 :- Transforming data
        secondTransformDF = OnlyAirpodsandIphoneTransformer().transform(inputDFs)

        # Step03 :- Loading data
        OnlyAirpodsAndIPhoneLoader(secondTransformDF).sink()



# COMMAND ----------

class Workflow:

    def __init__(self, name):
        self.name = name
    
    def runner(self):
        if self.name == "firstworkflow":
            return FirstWorkflow().runner()
        elif self.name == "secondworkflow":
            return SecondWorkflow().runner()
        else:
            raise ValueError("Not implemented for this workflow")

name = "secondworkflow"
workflow = Workflow(name).runner()



# COMMAND ----------

