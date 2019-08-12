"""
Example: spark-submit --master local ./etl_i94.py -y 2016 -m 4
"""
import os
import re
import sys, getopt

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import isnan, when, count, col, lit, ltrim, regexp_replace
from pyspark.sql.types import *


app_name = 'etl_i94'
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


def get_i94_df(spark, data_path, year, month, logger):
    """Create the dataframe of visitor data from the source file"""
    imm_data = os.path.join(data_path, "sas_data", "{:d}".format(year), "{:02d}".format(month), "*.csv")
    imm_df = spark.read.csv(imm_data, header=True)
    imm_df_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res',
                  'i94port', 'arrdate', 'i94mode', 'i94addr', 'depdate',
                  'i94bir', 'i94visa', 'matflag',
                 'biryear', 'gender', 'insnum', 'airline',
                 'admnum', 'fltno', 'visatype']

    for col in ['i94yr', 'i94mon', 'i94cit', 'i94res','i94mode',
                'i94bir', 'i94visa', 'biryear']:
                imm_df = imm_df.withColumn(col, imm_df[col].cast(IntegerType()))
    imm_df = imm_df.withColumn('fltno', regexp_replace('fltno', '^[0]*', ''))
    imm_df = imm_df.select(imm_df_cols)
    imm_df.persist()
    logger.info("imm_df row count: {}".format(imm_df.count()))
    return imm_df


def get_port_df(spark, data_path, logger):
    """Create the dataframe of port lookup data from the source file"""
    port_data = os.path.join(data_path, 'lookup_data', 'i94port_codes.csv')
    port_df = spark.read.csv(port_data, sep="|", header=True)
    port_df.persist()
    logger.info("port_df row count: {}".format(port_df.count()))
    return port_df


def get_ctry_df(spark, data_path, logger):
    """Creat the dataframe of country lookup data from the source file"""
    ctry_data = os.path.join(data_path, 'lookup_data', 'i94cit_i94res_codes.csv')
    ctry_df = spark.read.csv(ctry_data, sep='|', header=True)
    ctry_df = ctry_df.withColumn('code', ctry_df['code'].cast(IntegerType()))
    ctry_df.persist()
    logger.info("ctry_df row count: {}".format(ctry_df.count()))
    return ctry_df


def get_fltno_df(spark, data_path, logger):
    """Create the dataframe of existing flight numbers and airports from source file"""
    fltno_data = os.path.join(data_path, 'analytics_data', 'flight', 'flightno.csv')
    fltno_cols = ['airline', 'fltno', 'depapt', 'arrapt']
    fltno_df = spark.createDataFrame([('', '', '', '')], fltno_cols)
    try:
        fltno_df = (spark.read.csv(fltno_data, header=True)
                    .select(fltno_cols)
                    .dropDuplicates()
                    )
    except:
        pass
    fltno_df.persist()
    logger.info("fltno_df row count: {}".format(fltno_df.count()))
    return fltno_df


def merge_i94_lookup(spark, imm_df, port_df, ctry_df, fltno_df, logger):
    """Join against lookup data for port, citizenship and residency codes.
        Join against the flight numbers and airport data using airline and flight number.
        Return the resulting dataframe.
    """
    merge_df = (imm_df.join(port_df, imm_df.i94port==port_df.code, how='left')
            .drop(port_df.code)
            .join(ctry_df, imm_df.i94cit==ctry_df.code, how='left')
            .drop(ctry_df.code).withColumnRenamed('i94ctry', 'citizen_ctry')
            .join(ctry_df, imm_df.i94res==ctry_df.code, how='left')
            .drop(ctry_df.code).withColumnRenamed('i94ctry', 'resident_ctry')
            .join(fltno_df, (imm_df.airline==fltno_df.airline) & (imm_df.fltno == fltno_df.fltno), how='left')
            .drop(fltno_df.airline).drop(fltno_df.fltno)
           )

    merge_df.persist()
    logger.info("Merged i94 data with port, country and flight number data")
    return merge_df

def get_final_i94(spark, data_path, merge_df, logger):
    """Select the columns required for the analytics data set.
    """
    merge_df_cols = ['cicid', 'i94yr', 'i94mon',
                        'citizen_ctry', 'resident_ctry',
                        'i94port_raw', 'i94port_state', 'i94port_bps', 'i94port_city',
                        'arrdate', 'depdate',
                        'i94mode', 'i94addr', 'i94visa', 'visatype',
                        'i94bir', 'biryear', 'gender',
                        'matflag', 'insnum', 'admnum',
                        'airline', 'fltno'
                        ]
    merge_df = merge_df.select(merge_df_cols)
    merge_df.persist()
    logger.info("Selected final data set")
    return merge_df

def get_unknown_fltno(spark, data_path, merge_df, logger):
    """Find the unique and unknown flight numbers."""
    unknown_fltno_df = (merge_df.filter(merge_df.depapt.isNull())
                        .filter(merge_df.airline.isNotNull())
                        .filter(merge_df.fltno.isNotNull())
                        .select(['airline', 'fltno'])
                        .distinct())
    unknown_fltno_df.persist()
    logger.info("Selected unknown flight number data")
    return unknown_fltno_df

def write_i94(spark, data_path, merge_df, logger):
    """Write to parquet format."""
    i94_data = os.path.join(data_path, 'analytics_data', 'i94')
    merge_df.write.parquet(i94_data, mode='overwrite', partitionBy=['i94yr', 'i94mon'])
    logger.info("Wrote i94 data to parquet")


def write_unknown_fltno(spark, data_path, unknown_fltno_df, logger):
    """write the unknown flight number dataframe to the staging data area as csv."""
    unknown_fltno_data = os.path.join(data_path, 'staging_data','unknown_fltno')
    unknown_fltno_df.coalesce(1).write.csv(unknown_fltno_data, mode='overwrite', header=True)
    logger.info("Wrote unknown flight number data to csv")


def write_rowsandnulls(spark, data_path, data_dir, year, month, df, logger):
    """Write out the total number of rows plus counts of any nulls, nans, empty strings
        and unknown values to csv.
    """
    totRows = df.select(df.columns[0]).count()
    checknulls_df = (df.select([count(when(isnan(c), c)).alias(c) for c in df.columns])
                            .withColumn('checktype', lit('isnan'))
                            .withColumn('totalrows', lit(totRows))
                    .union(df
                            .select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
                            .withColumn('checktype', lit('isnull'))
                            .withColumn('totalrows', lit(totRows))
                            )
                    .union(df
                            .select([count(when(col(c) == '', c)).alias(c) for c in df.columns])
                            .withColumn('checktype', lit('emptystring'))
                            .withColumn('totalrows', lit(totRows))
                            )
                    .union(df
                            .select([count(when(col(c) == 'unknown', c)).alias(c) for c in df.columns])
                            .withColumn('checktype', lit('unknownstring'))
                            .withColumn('totalrows', lit(totRows))
                            )
                    .union(df
                            .select([count(when(col(c) == -1, c)).alias(c) for c in df.columns])
                            .withColumn('checktype', lit('nan_as_-1'))
                            .withColumn('totalrows', lit(totRows))
                            )
                    )
    checknulls_data = os.path.join(data_path, 'pipeline_logs',
                                    "{:d}".format(year), "{:02d}".format(month),
                                    data_dir, 'checknulls')
    checknulls_df.coalesce(1).write.csv(checknulls_data, mode='overwrite', header=True)
    logger.info("Wrote data summary for {} - checknulls".format(data_dir))


def write_intfield_summary(spark, data_path, data_dir, year, month, merge_df, logger):
    """Write out the summary statistics for each of the integer fields to csv."""
    int_describe_data = os.path.join(data_path, 'pipeline_logs',
                                    "{:d}".format(year), "{:02d}".format(month),
                                    data_dir, 'intfields')
    int_describe_df = (merge_df
                        .select('i94yr', 'i94mon', 'i94mode', 'i94bir',
                                        'biryear', 'i94visa', 'admnum')
                        .describe())
    int_describe_df.coalesce(1).write.csv(int_describe_data, mode='overwrite', header=True)
    logger.info("Wrote data summary - describe ints")


def write_stringfield_summary(spark, data_path, data_dir, year, month, merge_df, logger):
    """Write out a description of the string fields to csv"""
    string_describe_data = os.path.join(data_path, 'pipeline_logs',
                                        "{:d}".format(year), "{:02d}".format(month),
                                        data_dir, 'stringfields')
    string_describe_df = (merge_df
                        .select('i94addr', 'gender', 'airline', 'fltno',
                                    'visatype', 'i94port_state',
                                    'i94port_city', 'citizen_ctry', 'resident_ctry',
                                    'insnum', 'matflag')
                        .describe())
    string_describe_df = (string_describe_df
                        .filter(string_describe_df['summary'] != 'mean')
                        .filter(string_describe_df['summary'] != 'stddev'))
    string_describe_df.coalesce(1).write.csv(string_describe_data, mode='overwrite', header=True)
    logger.info("Wrote data summary - describe strings")



def main(argv):
    """Configure the input and output locations and call the processing methods"""

    spark = create_spark_session()
    logger = create_logger(spark)

    try:
        opts, args = getopt.getopt(argv,"y:m:",["year=","month="])
    except getopt.GetoptError:
        logger.info('test.py -y <year_int> -m <month_int>')
        raise Exception('Invalid argument to {}'.format(app_name))
    for opt, arg in opts:
        if opt in ("-y", "--year"):
            if arg.isnumeric():
                year = int(arg)
            else:
                raise Exception('Invalid year "{}" as argument to {}. Integer year required.'\
                                .format(arg, app_name))
        elif opt in ("-m", "--month"):

            if arg.isnumeric():
                month = int(arg)
            else:
                raise Exception('Invalid month "{}" as argument to {}. Integer month required.'\
                                .format(arg, app_name))
    logger.info('Year is {:d}'.format(year))
    logger.info('Month is {:d}'.format(month))

    #external_data = "s3a://udacity-dend/"
    #internal_data = "s3a://dysartcoal-dend-uswest2/capstone_test"

    #external_data = "s3a://dysartcoal-dend-uswest2/capstone_etl/data/"
    external_data = os.environ.get('HOME') + "/src/python/Udacity/CapstoneProject/prep/"
    internal_data = external_data

    i94_df = get_i94_df(spark, external_data, year, month, logger)
    i94_rowcount = i94_df.count()
    port_df = get_port_df(spark, internal_data, logger)
    ctry_df = get_ctry_df(spark, internal_data, logger)
    fltno_df = get_fltno_df(spark, internal_data, logger)
    merge_df = merge_i94_lookup(spark, i94_df, port_df, ctry_df, fltno_df, logger)
    unknown_fltno_df = get_unknown_fltno(spark, internal_data, merge_df, logger)

    final_df = get_final_i94(spark, internal_data, merge_df, logger)
    final_rowcount = final_df.count()
    assert i94_rowcount == final_rowcount, (("The row count has changed during processing: " +
                                        "initial rowcount={}, final rowcount={}")
                                        .format(i94_rowcount, final_rowcount))

    # Write analytics data
    write_unknown_fltno(spark, internal_data, unknown_fltno_df, logger)
    write_i94(spark, internal_data, final_df, logger)

    # Write out data summaries
    write_rowsandnulls(spark, internal_data, 'i94', year, month, merge_df, logger)
    write_rowsandnulls(spark, internal_data, 'flight', year, month, unknown_fltno_df, logger)
    write_intfield_summary(spark, internal_data,'i94', year, month, merge_df, logger)
    write_stringfield_summary(spark, internal_data, 'i94', year, month, merge_df, logger)


    logger.info("Finished etl_i94. Stopping spark.")
    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1:])
