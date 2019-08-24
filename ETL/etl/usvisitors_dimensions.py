import os
import re
import sys, getopt

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import (isnan, isnull, when, count, col, lit, ltrim,
                                   regexp_replace, concat, datediff, date_format,
                                   expr, trunc)
from pyspark.sql.types import *

app_name = 'usvisitors_dimensions'
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


def get_port_df(spark, data_path, logger):
    """Create the dataframe of port lookup data from the source file"""
    port_data = os.path.join(data_path, 'lookup_data', 'i94port_data.csv')
    port_df = spark.read.csv(port_data, header=True)
    port_df.persist()
    count = port_df.count()
    assert count > 0, "Zero rows in {}".format(port_data)
    logger.info("port_df row count: {}".format(count))
    return port_df

def get_port_dimension(spark, port_df, mode_df, logger):
    """Create the port dimension

    The identifier for the port is a combination of the port and the mode so that the different ports can
    be given individual attributes and plotted separately geographically if needs be in the future.
    """
    portdim_df = (mode_df.crossJoin(port_df))
    portdim_df = portdim_df.withColumn('port_id', concat(col('mode_code'), lit('_'), col('code')))
    portdim_df = portdim_df.withColumnRenamed('county_name', 'county')\
                                    .withColumnRenamed('state_name', 'state')\
                                    .withColumnRenamed('code', 'i94port_code')\
                                    .withColumnRenamed('country_code', 'country_id')
    portdim_cols = ['port_id', 'i94port_code', 'mode',
                        'latitude', 'longitude',
                        'place', 'city', 'county',
                        'state_id', 'state', 'country_id', 'country'
                       ]
    portdim_df = portdim_df.select(portdim_cols)
    portdim_df.persist()
    logger.info("portdim_df row count: {}".format(portdim_df.count()))
    return portdim_df

def get_age_dimension(spark, logger):
    """Create a dataframe to hold defined age ranges"""
    agedim_df = spark.createDataFrame([(0,'0-1'),
                                 (1, '2-10'),
                                 (2, '11-15'),
                                 (3, '16-20'),
                                 (4, '21-25'),
                                 (5, '26-35'),
                                 (6, '36-45'),
                                 (7, '46-55'),
                                 (8, '56-65'),
                                 (9, '66+'),
                                 (999, 'unknown')
                                 ], ['age_id', 'age_range'] )
    logger.info("agedim_df row count: {}".format(agedim_df.count()))
    return agedim_df


def get_duration_dimension(spark, logger):
    """Create a dataframe to hold defined durations"""
    durdim_df = spark.createDataFrame([(0,'0-3'),
                                (1, '4-7'),
                                (2, '8-10'),
                                (3, '11-14'),
                                (4, '15-21'),
                                (5, '22-28'),
                                (6, '29+'),
                                (999, 'unknown')
                                ], ['duration_id', 'duration_days'] )
    logger.info("durdim_df row count: {}".format(durdim_df.count()))
    return durdim_df


def write_dimension(spark, data_path, file_path, df, logger):
        df.write\
        .mode('overwrite')\
        .parquet(os.path.join(data_path, file_path))
        logger.info("Parquet format written to {}".format(file_path))


def main(argv):
    """Configure the input and output locations and call the processing methods"""

    spark = create_spark_session()
    logger = create_logger(spark)

    try:
        opts, args = getopt.getopt(argv,"p:",["path="])
    except getopt.GetoptError:
        logger.info('usvisitors_dimensions.py -p <path_to_data>')
        raise Exception('Invalid argument to {}'.format(app_name))
    for opt, arg in opts:
        if opt in ("-p", "--path"):
            data_path = arg

    logger.info('Path to data is {}'.format(data_path))

    port_df = get_port_df(spark, data_path, logger)
    mode_df = spark.createDataFrame([(1,'Air'),
                                    (2, 'Sea'),
                                    (3, 'Land'),
                                    (9, 'Not reported')],
                                    ['mode_code', 'mode'] )

    portdim_df = get_port_dimension(spark, port_df, mode_df, logger)
    agedim_df = get_age_dimension(spark, logger)
    durdim_df = get_duration_dimension(spark, logger)

    dest_path = 'analytics_data/us_visitors'
    write_dimension(spark, data_path, os.path.join(dest_path, 'port'), portdim_df, logger)
    write_dimension(spark, data_path, os.path.join(dest_path, 'age'), agedim_df, logger)
    write_dimension(spark, data_path, os.path.join(dest_path, 'duration'), durdim_df, logger)

    logger.info("Finished {}. Stopping spark.".format(app_name))
    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1:])
