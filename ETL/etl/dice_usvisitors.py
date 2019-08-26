"""Create a subset of the us_visitors data as a single csv file for analysis

The data is read from the visit_fact table joined against the relevant dimensions
and filtered according to the input parameters specifying the list of years,
months and states to be included.

A csv file is written which is named according to the years, months and states
specified.


Parameters:
    state (list of str): The states to be included
    year (list of int): The years to be included
    month (list of int): The months to be included
    data_path (str): Path to the data area for both input and output

Returns:
    An error is raised if the two values do not match.
    If the values match then the rowcounts are written to the pipeline logs
    for this run, under the rowcheck folder.

Example Usage:
Example: spark-submit --master local ./dice_usvisitors.py -p ../../prep/  -s [MI, AL, NC] -y [2016] -m [1,2,3]
"""
import os
import re
import sys, getopt

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import isnan, when, count, col, lit, ltrim, regexp_replace
from pyspark.sql.types import *


app_name = 'dice_usvisitors'
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

def get_slice(spark, data_path, yearlist, monthlist, statelist, logger):
    """Get the data for the given years, months and states """

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


    df = (visit_fact_df.where(col('year').isin(yearlist))
                        .where(col('month').isin(monthlist))
                        .where(col('arrivalstate_abbr').isin(statelist)))
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
                .drop(df.mode))
    slice_df.persist()
    rowcount = slice_df.count()

    logger.info(" slice for years {} months {} states {} Row Count: {}"\
                .format(yearlist, monthlist, statelist, rowcount))
    logger.info(slice_df.columns)
    return slice_df


def write_csv_slice(spark, data_path, yearlist, monthlist, statelist, df, logger):
    """Write the data to a single csv file"""
    separator='_'
    slice_years = 'yrs_' + str(yearlist)\
                    .replace('[','')\
                    .replace(']', '')\
                    .replace(',', separator)
    slice_months = 'mths_' + str(monthlist)\
                    .replace('[','')\
                    .replace(']', '')\
                    .replace(',', separator)
    slice_states = 'states_' + separator.join(statelist)
    slicename = slice_states + separator + slice_years + separator + slice_months
    slice_data = os.path.join(data_path, 'analysis', slicename)
    df.coalesce(1).write.csv(slice_data, mode='overwrite', header=True)
    logger.info("Wrote data slice")


def main(argv):
    """Configure the input and output locations and call the processing methods"""

    spark = create_spark_session()
    logger = create_logger(spark)

    try:
        opts, args = getopt.getopt(argv,"y:m:p:s:",["year=","month=","path=","state="])
    except getopt.GetoptError:
        logger.info('{}.py -y <listofyears> -m <listofmonths> -p <path_to_data> -s <listofstates>'\
                .format(app_name))
        raise Exception('Invalid argument to {}'.format(app_name))
    for opt, arg in opts:
        if opt in ("-y", "--year"):
            if (arg[0] == '[') & (arg[-1] == ']'):
                yearliststr = arg[1:-1].split(',')
                yearlist = [int(x) for x in yearliststr]
            else:
                raise Exception('Invalid year "{}" as argument to {}.py. List of years required. e.g. [2016]'\
                                .format(arg, app_name))
        elif opt in ("-m", "--month"):
            if (arg[0] == '[') & (arg[-1] == ']'):
                monthliststr = arg[1:-1].split(',')
                monthlist = [int(x) for x in monthliststr]
            else:
                raise Exception('Invalid month list "{}" as argument to {}.py. Integer month list required.'\
                                .format(arg, app_name))
        elif opt in ("-s", "--state"):
            if (arg[0] == '[') & (arg[-1] == ']'):
                statelist = arg[1:-1].split(',')
            else:
                raise Exception(('Invalid state list "{}" as argument to {}.py.' +
                                ' List of states required.')\
                                .format(arg, app_name))
        elif opt in ("-p", "--path"):
            data_path = arg

    logger.info('Yearlist is {}'.format(yearlist))
    logger.info('Monthlist is {}'.format(monthlist))
    logger.info('Statelist is {}'.format(statelist))
    logger.info('Path to data is {}'.format(data_path))

    slice_df = get_slice(spark, data_path, yearlist, monthlist, statelist, logger)
    write_csv_slice(spark, data_path, yearlist, monthlist, statelist, slice_df, logger)

    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1:])
