import os
from pyspark import SparkContext
from cbh_setup import CityblockSparkContext

cbh_sc = CityblockSparkContext(sc=SparkContext.getOrCreate())

sc = cbh_sc.context
spark = cbh_sc.session

print('\nBigQuery SparkContext available as \'sc\'.')
print('BigQuery SparkSession available as \'spark\'.')
