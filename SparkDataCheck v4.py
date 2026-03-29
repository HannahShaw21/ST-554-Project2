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
            return self
        else:
            if lower == None and upper == None:
                print("At least one of lower or upper must be provided")
            elif lower != None and upper == None:
                for i in column:
                    if column[i] == NULL: 
                        tfnvalue = "NULL" #tfn stands for "true/false/NULL"
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
            return self
    
    def check_string_values(self, column: str, values: list):
        '''
        A method that checks if each value in a string column falls within a user specified set/list of levels and returns the dataframe with an appended column of Boolean values
        '''
        #Note: not 100% sure what "levels" means and at this point it's too late to ask
        self.column = self.df[column]
        self.values = values
        self.booleans = []
        if self.df[column].dtype != "string":
            print("Column must be string type")
            return self
        else:
            for i in column:
                if column[i] == NULL: 
                    tfnvalue = "NULL"
                else:
                    tfnvalue = self.column.isin(self.values)
                self.booleans.append(tfnvalue)
            self.df = self.df.join[self.booleans]
            return self
                
    def check_null(self, column: str):
        '''
        A method that checks if each value in a column is missing (NULL specifically) and returns the dataframe with an appended column of Boolean values
        '''
        self.column = self.df[column]
        self.booleans = self.column.isNULL()
        self.df = self.df.join[self.booleans]
        return self

    #Summation methods
    
    def find_min_max(self, column: str = None, groupby: str = None):
        '''
         A method that reports the min and max of a numeric column supplied by the user. Includes an optional grouping variable (only one grouping variable allowed for simplicity). If no column is supplied, the method should report the min and max of any numeric columns (and produce no messages otherwise), grouped if appropriate
        '''
        self.column = self.df[column]
        self.groupby = self.column.groupby(groupby) 
        if self.df[column].dtype != "number":
            print("Column must be numeric type")
            return None
        elif column != None:
            col_min = self.groupby.min()
            col_max = self.groupby.max()
            print("Min =" col_min, "Max =" col_max)
        else:
            for name in self.df.columns
                if name.dtype = "number":
                    col_min = name.groupby(groupby).min()
                    col_max = name.groupby(groupby).max()
                    print("Min =" col_min, "Max =" col_max)
                else:
                    return None
            
    def count_string_columns(self, column1: str, column2: str = None):
        '''
        A method that should check if the column(s) are strings. If so, it should report the counts for the combinations of levels of each variable or of the single variable. If not, a message should be printed that the column is numeric.
        '''
        self.column1 = self.df[column1]
        self.column2 = self.df[column2]
        if self.column1.dtype != "string":
            count_of_levels1 = None
            print("Column 1 is numeric")
        else:
            count_of_levels1 = self.column1.value_counts()
        if column2 = None:
            count_of_levels2 = None
            return None
        elif self.column2.dtype != "string":
            count_of_levels2 = None
            print("Column 2 is numeric")
        else:
            count_of_levels2 = self.column2.value_counts()
        return count_of_levels1+count_of_levels2 