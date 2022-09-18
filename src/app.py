from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from operator import add
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
import requests
from bs4 import BeautifulSoup
import json

def main():
    print("This line will be printed@@@.")
    run_aggregation()
    testFunction()

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