"""Check that the original SAS i94 data input rows match the output us_visitors parquet rows

The count of input rows is read from the rowcount_udacitysas*.csv file for
the relevant month and year.

The sum of visitorcount column in the parquet data for the relevant year and month is
selected from the dataset.


Parameters:
    year (int): The year of the input data
    month (int): The month of the input data
    data_path (str): Path to the data area for both input and output

Returns:
    An error is raised if the two values do not match.
    If the values match then the rowcount and visitorcountsum are written to the pipeline logs
    for this run, under the rowcheck folder.

Example Usage:
Example: spark-submit --master local ./checkrowsandcounts_sas_parquet.py -y 2016 -m 1 -p ../../prep/
"""
import os
import re
import sys, getopt

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import (isnan, when, count, col, lit, ltrim,
                                    regexp_replace, isnull)
from pyspark.sql.types import *


app_name = 'checkrowsandcounts'
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


def get_usvisitors_count(spark, data_path, year, month, logger):
    """Get the visitor count for the fact transformed data for this year and month"""
    visit_fact_data = os.path.join(data_path, "analytics_data", "us_visitors/visit_fact")
    age_data = os.path.join(data_path, "analytics_data", "us_visitors/age")
    date_data = os.path.join(data_path, "analytics_data", "us_visitors/date")
    duration_data = os.path.join(data_path, "analytics_data", "us_visitors/duration")
    port_data = os.path.join(data_path, "analytics_data", "us_visitors/port")


    visit_fact_df = spark.read.parquet(visit_fact_data)
    age_df = spark.read.parquet(age_data)
    date_df = spark.read.parquet(date_data)
    duration_df = spark.read.parquet(duration_data)
    port_df = spark.read.parquet(port_data)

    df = visit_fact_df.filter(visit_fact_df['year'] == year).filter(visit_fact_df['month']==month)
    slice_df = (df.join(age_df, df.age_id==age_df.age_id)
                .drop(age_df.age_id)
                .join(date_df, df.arrivaldate_id==date_df.date_id)
                .drop(date_df.date_id)
                .join(duration_df, df.visitduration_id==duration_df.duration_id)
                .drop(duration_df.duration_id)
                .join(port_df, df.port_id==port_df.port_id)
                .drop(port_df.port_id)
                .drop(df.year)
                .drop(df.month)
                .drop(df.mode)
                .select('visitorcount'))
    slice_df.persist()

    sumvisitorcount = slice_df.groupBy().sum('visitorcount').collect()[0][0]

    logger.info("fact sum of visitorcount for year {:d} month {:02d}: {}"\
                .format(year, month, sumvisitorcount))
    return sumvisitorcount


def write_rowcheck(spark, data_path, data_dir, year, month, df, logger):
    """Write out the rowcheck to csv."""
    rowcheck_data = os.path.join(data_path, 'pipeline_logs', data_dir,
                                    "{:d}".format(year), "{:02d}".format(month),
                                    'rowcheck')
    df.coalesce(1).write.csv(rowcheck_data, mode='overwrite', header=True)
    logger.info("Wrote data summary - rowcheck")



def main(argv):
    """Configure the input and output locations and call the processing methods"""

    spark = create_spark_session()
    logger = create_logger(spark)

    try:
        opts, args = getopt.getopt(argv,"y:m:p:",["year=","month=","path="])
    except getopt.GetoptError:
        logger.info('{}.py -y <year_int> -m <month_int> -p <path_to_data>'.format(app_name))
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
    sumvisitorcount = get_usvisitors_count(spark, data_path, year, month, logger)

    assert(sas_rowcount == sumvisitorcount), "Rowcounts do not match sas: {} and fact {}"\
                                            .format(sas_rowcount, sumvisitorcount)
    cols = ['sas_rowcount', 'sumvisitorcount']
    df = spark.createDataFrame([(sas_rowcount, sumvisitorcount)], cols)

    write_rowcheck(spark, data_path,'us_visitors', year, month, df, logger)

    logger.info("Finished {}. Stopping spark.".format(app_name))
    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1:])
