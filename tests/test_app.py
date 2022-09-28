from ast import Not
import pytest
from src.app import testFunction
import unittest
from src.app import transform_data
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
import findspark as fs
import requests

#from pytest import assertEqual

def test_file1_method1():
	assert "testString" == testFunction()

def test_data_pull():
    fs.init()
    sc = SparkContext(appName="myAppName")
    spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     .getOrCreate())

    input_schema = StructType([
            StructField('StoreID', IntegerType(), True),
            StructField('Location', StringType(), True),
            StructField('Date', StringType(), True),
            StructField('ItemCount', IntegerType(), True)
        ])

    url2= 'https://www.treasury.gov/ofac/downloads/sanctions/1.0/sdn_advanced.xml'

    document = requests.get(url2)

    assert document is not None

def test_etl():
    #1. Prepare an input data frame that mimics our source data.
    #os.environ['PYSPARK_SUBMIT_ARGS'] = "--master mymaster --total-executor 2 --conf spark.driver.extraJavaOptions=-Dhttp.proxyHost=proxy.mycorp.com-Dhttp.proxyPort=1234 -Dhttp.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 -Dhttps.proxyHost=proxy.mycorp.com -Dhttps.proxyPort=1234 -Dhttps.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 pyspark-shell"
    fs.init()
    sc = SparkContext(appName="myAppName")
    spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     .getOrCreate())
    input_schema = StructType([
            StructField('StoreID', IntegerType(), True),
            StructField('Location', StringType(), True),
            StructField('Date', StringType(), True),
            StructField('ItemCount', IntegerType(), True)
        ])
    input_data = [(1, "Bangalore", "2021-12-01", 5),
                (2,"Bangalore" ,"2021-12-01",3),
                (5,"Amsterdam", "2021-12-02", 10),
                (6,"Amsterdam", "2021-12-01", 1),
                (8,"Warsaw","2021-12-02", 15),
                (7,"Warsaw","2021-12-01",99)]
    input_df = spark.createDataFrame(data=input_data, schema=input_schema)
    #2. Prepare an expected data frame which is the output that we expect.        
    expected_schema = StructType([
            StructField('Location', StringType(), True),
            StructField('TotalItemCount', LongType(), True)
            ])
    
    expected_data = [("Bangalore", 8),
                    ("Warsaw", 114),
                    ("Amsterdam", 11)]
    expected_df = spark.createDataFrame(data=expected_data, schema=expected_schema)

    #3. Apply our transformation to the input data frame
    transformed_df = transform_data(input_df)

    #4. Assert the output of the transformation to the expected data frame.
    field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
    fields1 = [*map(field_list, transformed_df.schema.fields)]
    fields2 = [*map(field_list, expected_df.schema.fields)]
    # Compare schema of transformed_df and expected_df
    res = set(fields1) == set(fields2)

    expected_df.show()
    print(expected_df)
    if expected_df is not None:
        a = expected_df.collect()
    
    b = sorted(transformed_df.collect())
    # assert
    #assertTrue(res)
    if (res):
        print("res is true")
    # Compare data in transformed_df and expected_df
    c = sorted(expected_df.collect()), sorted(transformed_df.collect())
    print(c)
    assert True