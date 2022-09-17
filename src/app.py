from pyspark.sql.types import *
from pyspark.sql.functions import *
import json

def main():
    print("This line will be printed@@@.")
    testFunction()

def testFunction():
    #print("inside function 2")
    return "testString"

if __name__ == "__main__":
    main()

def transform_data(input_df):
    transformed_df = (input_df.groupBy('Location',).agg(sum('ItemCount').alias('TotalItemCount')))
    return transformed_df