import pyspark
import logging as logger
sc = pyspark.SparkContext('local[*]')

txt = "EMR ran successfully 4485737"
logger.info(txt)
