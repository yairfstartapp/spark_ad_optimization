import sys
import os
import time

import datetime
from datetime import timedelta
from collections import namedtuple
import yaml
import pprint
import sqlalchemy as sa

dsn = 'vertica'

vertica_query = """ """


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
            .setAppName("read Vertica")
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
    #if config.WINDOWS_RUN:
    #    conf.set("spark.jars", "C:\Vertica\\vertica-jdbc-8.1.1-7.jar:C:\Vertica\\vertica-9.0.1_spark2.1_scala2.11.jar")
    #    conf.set("spark.driver.extraClassPath", "C:\Vertica\\vertica-jdbc-8.1.1-7.jar:C:\Vertica\\vertica-9.0.1_spark2.1_scala2.11.jar")
    #    conf.set("spark.executor.extraClassPath","C:\Vertica\\vertica-jdbc-8.1.1-7.jar:C:\Vertica\\vertica-9.0.1_spark2.1_scala2.11.jar")



    sc = SparkContext(conf=conf)
    sc.setLogLevel('INFO')

    #if not config.EMR_MODE :
     #   sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", "AKIAJDEN7GCOGKF7LFIA")
     #   sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey","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")
     #   sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    sqlContext = SQLContext(sparkContext=sc)
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("read_Vertica").setLevel( logger.Level.INFO )
    loggerInst = logger.LogManager.getLogger("read_Vertica")

    opts = {}
    opts['table'] = "adEvents"

    opts['db'] = "startapp"

    opts['user'] = "YairF"

    opts['password'] = "pass4U!"

    opts['host'] = "vertica44"

    opts['part'] = "12";

    #opt = [('host', host), ('table', table), ('db', db), ('numPartitions', part), ('user', user), ('password', password)]

    df = sqlContext.read.format("com.vertica.spark.datasource.DefaultSource").options(**opts).load()
    #props = {"driver": "com.vertica.jdbc.Driver"}
    #df = sqlContext.read.jdbc(
    #    url="jdbc:vertica://"+opts['host']+":/"+opts['db']+"?user="+opts['user']+"&password="+opts['password'],
    #    table="select date(requestTs) as eDate ,campaignId FROM adEvents  WHERE date(requestTs) >= CURRENT_DATE - 1 AND date(requestTs) <= CURRENT_DATE  limit 10",
    #            properties=props
    #            ).load()
    #c = df.select("ts").filter("ts > cast('2018-03-30' as date)").show(5)
    df.show(5)

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