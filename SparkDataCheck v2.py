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
    
    #Validation methods
    
    def check_within_num_range(self, column: str, lower: str = None, upper: str = None):
        '''
        A method that checks if each value in a numeric column is within user defined limits (upper and lower bounds, inclusive) and returns the dataframe with an appended column of Boolean values
        '''
        self.column = self.df[column]
        self.lower = lower
        self.upper = upper
        self.booleans = []
        if self.df[column].dtype != "number":
            print("Column must be numeric type")
        else:
            if lower == None and upper == None:
                print("At least one of lower or upper must be provided")
            elif lower != None and upper == None:
                for i in column:
                    if column[i] == NULL: 
                        tfnvalue = "NULL"
                    else:
                        tfnvalue = bool(column[i] >= lower)
                    self.booleans.append(tfnvalue)
            elif lower == None and upper != None:
                for i in column:
                    if column[i] == NULL: 
                        tfnvalue = "NULL"
                    else:
                        tfnvalue = bool(column[i] <= upper)
                    self.booleans.append(tfnvalue)
            else:
                for i in column:
                    if column[i] == NULL: 
                        tfnvalue = "NULL"
                    else:
                        tfnvalue = bool(column[i] >= lower and column[i] <= upper)
                    self.booleans.append(tfnvalue)
            self.df = self.df.join[self.booleans]
            return self.df
    
    #Create a method that checks if each value in a string column falls within a user specified set of levels and returns the dataframe with an appended column of Boolean values
    
    
    #Create a method that checks if a each value in a column is missing (NULL specifically) and returns the dataframe with an appended column of Boolean values