"""Check that the original SAS i94 data input rows match the output i94 parquet rows

The count of input rows is read from the rowcount_udacitysas*.csv file for
the relevant month and year.

The count of rows in the parquet data for the relevant month and row is
selected from the dataset.


Parameters:
    year (int): The year of the input data
    month (int): The month of the input data
    data_path (str): Path to the data area for both input and output

Returns:
    An error is raised if the two values do not match.
    If the values match then the rowcounts are written to the pipeline logs
    for this run, under the rowcheck folder.

Example Usage:
Example: spark-submit --master local ./checki94rows_sas_parquet.py -y 2016 -m 1 -p ../../prep/
"""
import os
import re
import sys, getopt

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import isnan, when, count, col, lit, ltrim, regexp_replace
from pyspark.sql.types import *


app_name = 'check_i94rows'
def create_spark_session():
    """Creates the spark session"""
    spark = SparkSession\
    .builder\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .appName(app_name)\
    .getOrCreate()
    return spark


def create_logger(spark):
    """Creates the logger """
    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info("pyspark script logger initialized")
    return logger


def get_sas_rowcount(spark, data_path, year, month, logger):
    """Get the rowcount for the sas data for this year and month"""
    sas_data = os.path.join(data_path, "sas_data", "rowcount_udacitysas_{:d}_{:02d}.csv".format(year, month))
    sas_rc_df = spark.read.csv(sas_data, header=True)
    sas_rc_df.persist()
    assert(sas_rc_df.count() == 1), 'There must only be a single row in {}'.format(sas_data)
    sas_rowcount = sas_rc_df.select('sas_data_rowcount').collect()[0][0]
    sas_rowcount = int(sas_rowcount)
    logger.info("sas row count for year {:d} month {}:02d: {}".format(year, month, sas_rowcount))
    return sas_rowcount

def get_i94_rowcount(spark, data_path, year, month, logger):
    """Get the rowcount for the i94 transformed data for this year and month"""
    i94_data = os.path.join(data_path, "analytics_data", "i94")
    i94_df = spark.read.parquet(i94_data)
    i94_rowcount = i94_df.filter(i94_df['i94yr'] == year).filter(i94_df['i94mon']==month).count()
    logger.info("i94 row count for year {:d} month {}:02d: {}".format(year, month, i94_rowcount))
    return i94_rowcount

def write_rowcheck(spark, data_path, data_dir, year, month, df, logger):
    """Write out the rowcheck to csv."""
    rowcheck_data = os.path.join(data_path, 'pipeline_logs',
                                    "{:d}".format(year), "{:02d}".format(month),
                                    data_dir, 'rowcheck')
    df.coalesce(1).write.csv(rowcheck_data, mode='overwrite', header=True)
    logger.info("Wrote data summary - rowcheck")



def main(argv):
    """Configure the input and output locations and call the processing methods"""

    spark = create_spark_session()
    logger = create_logger(spark)

    try:
        opts, args = getopt.getopt(argv,"y:m:p:",["year=","month=","path="])
    except getopt.GetoptError:
        logger.info('etl_i94.py -y <year_int> -m <month_int> -p <path_to_data>')
        raise Exception('Invalid argument to {}'.format(app_name))
    for opt, arg in opts:
        if opt in ("-y", "--year"):
            if arg.isdigit():
                year = int(arg)
            else:
                raise Exception('Invalid year "{}" as argument to {}. Integer year required.'\
                                .format(arg, app_name))
        elif opt in ("-m", "--month"):

            if arg.isdigit():
                month = int(arg)
            else:
                raise Exception('Invalid month "{}" as argument to {}. Integer month required.'\
                                .format(arg, app_name))
        elif opt in ("-p", "--path"):
            data_path = arg

    logger.info('Year is {:d}'.format(year))
    logger.info('Month is {:d}'.format(month))
    logger.info('Path to data is {}'.format(data_path))


    sas_rowcount = get_sas_rowcount(spark, data_path, year, month, logger)
    i94_rowcount = get_i94_rowcount(spark, data_path, year, month, logger)

    logger.info('type of sas_rowcount: {}'.format(type(sas_rowcount)))
    logger.info('type of i94_rowcount: {}'.format(type(i94_rowcount)))
    assert(sas_rowcount == i94_rowcount), "Rowcounts do not match sas: {} and i94 {}".format(sas_rowcount, i94_rowcount)
    cols = ['sas_rowcount', 'i94_rowcount']
    df = spark.createDataFrame([(sas_rowcount, i94_rowcount)], cols)

    write_rowcheck(spark, data_path,'i94', year, month, df, logger)

    logger.info("Finished {}. Stopping spark.".format(app_name))
    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1:])
