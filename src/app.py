from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from operator import add
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
import requests #todo: make shell scrip to install this
from bs4 import BeautifulSoup
import json
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
import findspark as fs


def main():
    print("This line will be printed@@@.")
    run_aggregation()
    testFunction()
    createDataFrame()

def createDataFrame():
    fs.init()
    sc = SparkContext(appName="SDN-Aggregator")
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

def testFunction():
    print("inside function 2")
    return "testString"

def transform_data(input_df):
    transformed_df = (input_df.groupBy('Location',).agg(sum('ItemCount').alias('TotalItemCount')))
    return transformed_df

def run_aggregation():
    url = 'https://www.w3schools.com/xml/simple.xml'

    url2= 'https://www.treasury.gov/ofac/downloads/sanctions/1.0/sdn_advanced.xml'

    document = requests.get(url2)

    soup=BeautifulSoup(document.content,"lxml-xml")
#distinctParties = soup.findAll("DistinctParty")
#print("****distinct parties****")
#print(distinctParties.__len__())
#for party in distinctParties:
#for i in range(5):
#    party = distinctParties[i]
#    print("****party****" + str(i))
#    print(party)
#    print("****party****" + str(i))


    sanctionsEntries = soup.findAll("SanctionsEntry")
    print("****distinct sanctions****")
    print(sanctionsEntries.__len__())
    #for party in distinctParties:
    for i in range(5):
        party = sanctionsEntries[i]
        print("****party****" + str(i))
        print(party)
        print("****party****" + str(i))



#import pandas as pd
#from pandas.util.testing import makeTimeSeries

#df = makeTimeSeries()
#df.head()

#from pyspark.sql import SparkSession
#spark = SparkSession.builder.getOrCreate()
#foo = spark.read.parquet('s3a://<some_path_to_a_parquet_file>')
# Create a spark session
#spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
 
# Create an empty RDD
#emp_RDD = spark.sparkContext.emptyRDD()
 
# Create empty schema
#columns = StructType([])
 
# Create an empty RDD with empty schema
#data = spark.createDataFrame(data = emp_RDD,
#                             schema = columns)

#
#df = spark.read.format("com.databricks.spark.xml") \
#    .option("rowTag","record").load("https://www.treasury.gov/ofac/downloads/sanctions/1.0/sdn_advanced.xml")

#file_rdd = spark.read.text("./data/*.xml", wholetext=True).rdd


# Print the dataframe
#print('Dataframe :')
#df.show()
 
# Print the schema
#print('Schema :')
#df.printSchema()


if __name__ == "__main__":
    main()