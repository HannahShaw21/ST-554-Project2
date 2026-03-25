from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd
import importlib

class SparkDataCheck:
    
    '''
    A class for creating and modifying instances of Spark SQL data frames from CSV files
    '''
    
    def __init__(self, df: DataFrame):
        self.df = df
        
    #creates an instance while reading in a csv file
    @classmethod
    def instance_readcsv(cls, spark, csv_file): 
        csv = spark.read.load(csv_file, format="csv", sep=";", inferSchema="true", header="true")
        return cls(csv)
        
    #creates an instance from a pandas dataframe
    @classmethod
    def instance_pandas(cls, spark, pandas_df): 
        df = spark.CreateDataFrame(pandas_df)
        return cls(df)