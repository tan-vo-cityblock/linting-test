from cbh_setup import CityblockSparkContext
from pyspark.sql import Row

fruits = [
    Row(name='orange', color='orange', quantity=4),
    Row(name='lemon', color='yellow', quantity=8),
    Row(name='apple', color='red', quantity=1),
    Row(name='blood orange', color='orange but also kind of red', quantity=3)
]

if __name__ == '__main__':
    spark = CityblockSparkContext(app_name='BigQuery IO Test').session
    df = spark.createDataFrame(fruits)

    print('Writing a simple table to BigQuery')
    df.write.format('bigquery') \
        .mode('overwrite') \
        .option('table', 'spark_test.bigquery_io_test') \
        .save()

    print('Reading a simple table from BigQuery')
    result = spark.read.format('bigquery') \
        .option('table', 'spark_test.bigquery_io_test') \
        .load()

    if result.count() != len(fruits):
        raise Exception('Test failed.')

    spark.stop()
    print('Successfully wrote to and then read from BigQuery!')
