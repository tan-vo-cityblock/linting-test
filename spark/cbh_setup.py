import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


class CityblockSparkContext:
    def __init__(self, sc=None, app_name=None):
        self.__local = 'CBH_SPARK_LOCAL' in os.environ
        self.__project_id = os.environ['CBH_SPARK_PROJECT_ID']
        self.__bucket = os.environ['CBH_SPARK_TEMP_BUCKET']
        self.__ncpu = os.environ['CBH_SPARK_NCPU']

        if self.__local and 'CBH_SPARK_SVC_ACCT_KEYFILE' not in os.environ:
            raise Exception('Please set "CBH_SPARK_SVC_ACCT_KEYFILE" when using local spark executor.')
        else:
            self.__keyfile = os.environ['CBH_SPARK_SVC_ACCT_KEYFILE']

        conf = SparkConf() \
            .setMaster(f'local[{self.__ncpu}]') \
            .setAppName(app_name if app_name is not None else 'Cityblock PySpark Job')

        if 'CBH_USE_JUPYTER' in os.environ:
            conf.set('spark.jars.packages', os.environ['CBH_SPARK_PACKAGES'])

        self.context = sc if sc is not None else SparkContext(conf=conf).getOrCreate()

        if self.__local:
            hadoop_conf = self.context._jsc.hadoopConfiguration()
            hadoop_conf.set('fs.gs.project.id', self.__project_id)
            hadoop_conf.set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
            hadoop_conf.set('fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS')
            hadoop_conf.set('google.cloud.auth.service.account.enable', 'true')
            hadoop_conf.set('google.cloud.auth.service.account.json.keyfile', self.__keyfile)

        builder = SparkSession.builder \
            .config(conf=self.context.getConf()) \
            .config('temporaryGcsBucket', self.__bucket)

        if self.__local:
            builder.config('spark.sql.shuffle.partitions', self.__ncpu) \

        self.session = builder.getOrCreate()
