import sys
import os
import time

import datetime
from datetime import timedelta
from collections import namedtuple
import yaml
import pprint


def get_config( config_path=r'C:\Users\Yair\PycharmProjects\ad_optimization\spark\load_s3.yml'):
    config_dct = yaml.load(open(config_path))
    Config = namedtuple('Config', config_dct.keys())
    return Config(**config_dct)

def load_s3_data(**kw):
    config = get_config(**kw)
    pp = pprint.PrettyPrinter(indent=4)

    print ("Successfully got config ")
    pp.pprint(config)

    try:
        from pyspark import SparkContext
        from pyspark import SparkConf
        from pyspark import SQLContext

        print ("Successfully imported Spark Modules -- `SparkContext, SQLContext`")

    except ImportError as e:
        print ("Can not import Spark Modules", e)
        sys.exit(1)

    #master_path = 'spark://' + '10.100.101.158' + ':7077'
    #master_path = 'spark://' + '10.100.101.176' + ':7077'

    t_start = time.time()

    conf = (SparkConf()
            .setAppName("read s3 for training App")
            .setMaster('spark://' + config.SPARK_MASTER + ':7077')
            .set("spark.hadoop.parquet.enable.summary-metadata","false")
            .set("spark.sql.parquet.mergeSchema","false")
            .set("spark.sql.parquet.filterPushdown","true")
            .set("spark.sql.hive.metastorePartitionPruning" ,"true")
    #TODO: check this (from https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.1/bk_cloud-data-access/content/s3-sequential-reads.html)
            # spark.hadoop.fs.s3a.readahead.range 512M
            .set("spark.executor.cores", config.EXECUTOR_CORE_NUM)
            .set("spark.executor.memory", config.EXECUTOR_MEMORY))
    #print os.environ
    if config.WINDOWS_RUN:
        conf.set("spark.jars", "..\..\..\..\SparkJars\aws-java-sdk-1.10.34.jar:..\..\..\..\SparkJars\hadoop-aws-2.7.1.jar")



    sc = SparkContext(conf=conf)
    sc.setLogLevel('INFO')

    if not config.EMR_MODE :
        sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", "AKIAJDEN7GCOGKF7LFIA")
        sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey","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")
        sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    sqlContext = SQLContext(sparkContext=sc)
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("read_s3_for_training").setLevel( logger.Level.INFO )
    loggerInst = logger.LogManager.getLogger("read_s3_for_training")

    #UNCOMMENT !
    #start_date = datetime.datetime.now() - timedelta(days=7)
   # end_date = datetime.datetime.now()
    # COMMENT !
    start_date = datetime.date(2013, 02, 28)
    end_date = datetime.date(2013,03,05)

    n = (end_date - start_date).days + 1

    base_path = 'vertica-parquet-data/agg.adEventsAgg/'

    paths = [
        's3://'+base_path+'edate='+"{}-{:02d}-{:02d}/*".format(d.year, d.month, d.day)
        for d in [start_date + datetime.timedelta(days=i) for i in range(n)]
    ]

    # Loads parquet file located in local file system into RDD Data Frame
    #parquetFile = sqlContext.read.parquet('s3://vertica-parquet-data/agg.adEventsAgg/edate=2013-02-19/d5140819-v_startapp_node0012-139917388596992.parquet')
    parquetFile = sqlContext.read.option("basePath",'s3://'+base_path).load(paths)

    # Stores the DataFrame into an "in-memory temporary table"
    parquetFile.registerTempTable("parquetFile")

    # Run standard SQL queries against temporary table
    nations_all_sql = sqlContext.sql("SELECT * FROM parquetFile")

    nations_all_sql.show(5)

    print('Run time: {}'.format(timedelta(seconds=time.time() - t_start)))



if __name__ == '__main__':
    print (sys.argv[1])
    if len(sys.argv) == 0:
        raise EnvironmentError('No input in argv, you should at least pass path template.')
    if len(sys.argv) == 2:
        load_s3_data(config_path=sys.argv[1])


# Print the result set
#nations_all = nations_all_sql.map(lambda p: "Country: {0:15} Ipsum Comment: {1}".format(p.name, p.comment_col))

#print("All Nations and Comments -- `SELECT * FROM parquetFile`")

# Use standard SQL to filter
#nations_filtered_sql = sqlContext.sql("SELECT name FROM parquetFile WHERE name LIKE '%UNITED%'")

# Print the result set
#nations_filtered = nations_filtered_sql.map(lambda p: "Country: {0:20}".format(p.name))

#print_horizontal()
#loggerInst.info("Nations Filtered -- `SELECT name FROM parquetFile WHERE name LIKE '%UNITED%'`")
#print_horizontal()
#for nation in nations_filtered.collect():
#    loggerInst.info(nation)
#    loggerInst.info(nation)