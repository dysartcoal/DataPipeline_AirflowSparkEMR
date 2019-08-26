"""Extract source i94 data for the year and month, aggregate and write to S3 as parquet

The date dimension for the given month and year is generated and written
as parquet format to S3.

i94 data is read in (from the CSV files) for the specified year and month.
The i94 data is merged with
   - port codes
   - country codes for citizenship and country of residence

The i94 data is also processed to identify the age_id for accurate joining with
the age dimension and visitduration_id for accurate joining with the duration
dimension.

The data set is analysed for missing values, nulls and unknowns prio to aggregation and the
summary data for this is written to a pipeline log file.

The data is aggregated to enable analysis of the count of visitors according
to selected fields.

This output data is intended for initial data analysis to verify the usefulness
of the data warehouse.  If successful, the data is intended for analysis of
the visitor market to the US and in particular for visualising the geographically
distribution.

Parameters:
    year (int): The year of the input data
    month (int): The month of the input data
    data_path (str): Path to the data area for both input and output

Returns:

Example Usage:
spark-submit --master local ./usvisitors_fact.py -y 2016 -m 1 -p ../../prep/
"""
import os
import re
import sys, getopt
import datetime

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import (isnan, isnull, when, count, col, lit, ltrim,
                                   regexp_replace, concat, datediff, date_format,
                                   add_months, date_add, expr, trunc, coalesce,
                                  desc)
from pyspark.sql.types import *
from pyspark.ml.feature import Bucketizer


app_name = 'usvisitors_fact'
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


def get_date_dimension(spark, year, month, logger):
    """Create the dataframe for the date dimension for this year and month"""
    start_date = datetime.date(int(year), int(month), 1)
    basedates_df =  spark.createDataFrame([(start_date,)], ['start_date'])
    basedates_df = basedates_df.withColumn('end_date',
                                        add_months(basedates_df.start_date, 1))
    end_date = basedates_df.collect()[0][1]
    numdays = (end_date - start_date).days

    datelist = [start_date + datetime.timedelta(days=x) for x in range(numdays)]
    datesofmonth_df =  spark.createDataFrame(datelist, DateType())\
                            .withColumnRenamed('value', 'date')

    date_df = datesofmonth_df.withColumn('str_weekday',
                                    date_format(datesofmonth_df.date, 'EEEE'))\
                            .selectExpr("date as date_id",
                                        "day(date) as day",
                                     "weekofyear(date) as week",
                                     "month(date) as month",
                                     "year(date) as year",
                                     "str_weekday as weekday")\
                            .distinct()\
                            .orderBy('date_id')
    date_df.persist()
    logger.info("date_df row count: {}".format(date_df.count()))
    return date_df


def write_date_dimension(spark, data_path, file_path, df, logger):
    """Write the date dimension to parquet format and append to existing data."""
    df.coalesce(1)\
    .write\
    .mode('append')\
    .parquet(os.path.join(data_path, file_path, 'date'))
    logger.info("Parquet format appended to {}".format(file_path))


def get_i94_df(spark, data_path, year, month, logger):
    """Create the dataframe of visitor data from the source file"""
    imm_data = os.path.join(data_path, "sas_data", "{:d}".format(year),
                            "{:02d}".format(month), "*.csv")
    imm_df = spark.read.csv(imm_data, header=True)
    imm_df.persist()
    count = imm_df.count()
    assert count > 0, "Zero rows in {}".format(imm_data)
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
    logger.info("imm_df row count: {}".format(count))
    return imm_df


def get_port_df(spark, data_path, logger):
    """Create the dataframe of port dimension data from the source file"""
    port_data = os.path.join(data_path, 'analytics_data','us_visitors', 'port')
    port_df = spark.read.parquet(port_data).select('port_id', 'i94port_code',
                                                'port_mode', 'port_state', 'port_state_abbr')
    port_df = port_df.withColumnRenamed('port_state', 'arrivalstate')\
                    .withColumnRenamed('port_state_abbr', 'arrivalstate_abbr')\
                    .withColumnRenamed('port_mode', 'mode')
    port_df.persist()
    count = port_df.count()
    assert count > 0, "Zero rows in {}".format(port_data)
    logger.info("port_df row count: {}".format(count))
    return port_df


def get_dest_df(spark, data_path, logger):
    """Create the dataframe of destination states from the source file"""
    dest_data = os.path.join(data_path, 'lookup_data', 'i94addr_codes.csv')
    dest_df = spark.read.csv(dest_data, header=True)
    dest_df.persist()
    count = dest_df.count()
    assert count > 0, "Zero rows in {}".format(dest_data)
    logger.info("dest_df row count: {}".format(count))
    return dest_df


def get_ctry_df(spark, data_path, logger):
    """Create the dataframe of country lookup data from the source file"""
    ctry_data = os.path.join(data_path, 'lookup_data', 'i94cit_i94res_codes.csv')
    ctry_df = spark.read.csv(ctry_data, sep='|', header=True)
    ctry_df = ctry_df.withColumn('code', ctry_df['code'].cast(IntegerType()))
    ctry_df.persist()
    count = ctry_df.count()
    assert count > 0, "Zero rows in {}".format(ctry_data)
    logger.info("ctry_df row count: {}".format(count))
    return ctry_df


def merge_i94_lookup(spark, df, port_df, ctry_df, dest_df, mode_df, visa_df, logger):
    """Join against the dimensions to create the raw fact data pre-aggregation

        Join against the port dimension to get the port_id and arrival state.
        Join against the ctry_df to get the country of residence.
        Join aginst the dest_df to get the first destination state.
        Join against the mode_df to get the text definition for the mode of arrival.
        Join against the visa_df to get the text definition for the purpose of visit.
        Return the resulting dataframe.
    """
    merge_df = (df.join(visa_df, df.i94visa==visa_df.purpose_code, how='left')
                .drop(visa_df.purpose_code)
                .withColumnRenamed('purpose', 'visitpurpose')
                .join(mode_df, df.i94mode==mode_df.mode_code, how='left')
                .drop(mode_df.mode_code)
                .join(port_df, (df.i94port==port_df.i94port_code) &
                                (mode_df.mode == port_df.mode),
                                how='left')
                .drop(port_df.i94port_code)
                .drop(port_df.mode)
                .join(dest_df, df.i94addr==dest_df.state_id, how='left')
                .withColumnRenamed('state_id', 'firstdeststate_abbr')
                .withColumnRenamed('state_name', 'firstdeststate')
                .join(ctry_df, df.i94cit==ctry_df.code, how='left')
                .drop(ctry_df.code).withColumnRenamed('i94ctry', 'citizen_ctry')
                .join(ctry_df, df.i94res==ctry_df.code, how='left')
                .drop(ctry_df.code).withColumnRenamed('i94ctry', 'countryofresidence')
           )
    # Some values are populated but not in lookup data. Replace nulls with 'unknown'
    merge_df = (merge_df.withColumn("firstdeststate_abbr",
                        coalesce(merge_df.firstdeststate_abbr, lit('unknown')))
                        .withColumn("firstdeststate",
                        coalesce(merge_df.firstdeststate, lit('unknown')))
                        .withColumn("countryofresidence",
                        coalesce(merge_df.countryofresidence, lit('unknown')))
                        .withColumn("mode",
                        coalesce(merge_df.mode, lit('unknown')))
                        .withColumn("port_id",
                        coalesce(merge_df.port_id, lit('-1_XXX')))
                        .withColumn("arrivalstate",
                        coalesce(merge_df.arrivalstate, lit('unknown')))
                        .withColumn("arrivalstate_abbr",
                        coalesce(merge_df.arrivalstate_abbr, lit('unknown')))
               )
    merge_df.persist()
    logger.info("Merged i94 data with port and country data")
    return merge_df


def add_duration_id(spark, df, logger):
    """Calculate the visitduration_id by splitting the visit duration into buckets"""
    durdays_df = df.withColumn("duration_days", datediff("depdate", "arrdate"))
    ddbucketizer = Bucketizer(splits=[ float('-Inf'), 0, 4, 8, 11, 15, 22,
                                        29, float('Inf') ],
                                        inputCol="duration_days",
                        outputCol="ddbuckets")
    ddbuck_df = ddbucketizer.setHandleInvalid("keep").transform(durdays_df)
    dur_id_df = ddbuck_df.withColumn("visitduration_id",
                                   when(isnull(col("arrdate")) |
                                        isnull(col("depdate")), 999)\
                                   .otherwise(col("ddbuckets").cast(IntegerType()))
                                 )
    logger.info("Added duration_id")
    return dur_id_df


def add_age_id(spark, df, logger):
    """Calculate the age_id by splitting the visitor age into buckets"""
    agebucketizer = Bucketizer(splits=[ float('-Inf'), 0, 2, 11, 16, 21,
                                        26, 36, 46, 56, 66, float('Inf') ],
                                inputCol="i94bir",
                                outputCol="agebuckets")
    agebuck_df = agebucketizer.setHandleInvalid("keep").transform(df)
    age_id_df = agebuck_df.withColumn("age_id", when(col("i94bir") == -1, 999)\
                                                .otherwise(col("agebuckets")
                                                .cast(IntegerType()))
                                    )
    logger.info("Added age_id")
    age_id_df.persist()
    return age_id_df


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
    checknulls_data = os.path.join(data_path, data_dir,
                                    "{:d}".format(year), "{:02d}".format(month),
                                    'checknulls')
    checknulls_df.coalesce(1).write.csv(checknulls_data, mode='overwrite', header=True)
    logger.info("Wrote data summary for {} - checknulls".format(data_dir))


def write_intfield_summary(spark, data_path, data_dir, year, month, merge_df, logger):
    """Write out the summary statistics for each of the integer fields to csv."""
    int_describe_data = os.path.join(data_path, data_dir,
                                    "{:d}".format(year), "{:02d}".format(month),
                                    'intfields')
    int_describe_df = (merge_df
                        .select('i94yr', 'i94mon', 'i94mode', 'i94bir',
                                        'biryear', 'i94visa', 'admnum',
                                        'visitduration_id', 'age_id')
                        .describe())
    int_describe_df.coalesce(1).write.csv(int_describe_data, mode='overwrite', header=True)
    logger.info("Wrote data summary - describe ints")


def write_stringfield_summary(spark, data_path, data_dir, year, month, merge_df, logger):
    """Write out a description of the string fields to csv"""
    string_describe_data = os.path.join(data_path, data_dir,
                                        "{:d}".format(year), "{:02d}".format(month),
                                        'stringfields')
    string_describe_df = (merge_df
                        .select('i94addr', 'gender', 'airline', 'fltno',
                                    'visatype', 'visitpurpose',
                                    'mode', 'port_id',
                                    'arrivalstate', 'arrivalstate_abbr',
                                    'firstdeststate', 'firstdeststate_abbr',
                                    'citizen_ctry', 'countryofresidence',
                                    'insnum', 'matflag')
                        .describe())
    string_describe_df = (string_describe_df
                        .filter(string_describe_df['summary'] != 'mean')
                        .filter(string_describe_df['summary'] != 'stddev'))
    string_describe_df.coalesce(1).write.csv(string_describe_data, mode='overwrite', header=True)
    logger.info("Wrote data summary - describe strings")


def get_fact(spark, df, logger):
    """Aggregate the data by the fact columns."""
    fact_cols = ['arrivaldate_id', 'year', 'month', 'port_id',
                 'mode', 'visitduration_id',  'visitpurpose',
                 'arrivalstate', 'arrivalstate_abbr',
                 'firstdeststate', 'firstdeststate_abbr',
                 'age_id', 'gender', 'countryofresidence']
    factdetails_df = (df.withColumnRenamed('arrdate', 'arrivaldate_id')
                  .withColumnRenamed('i94yr', 'year')
                  .withColumnRenamed('i94mon', 'month')
                  .select(fact_cols))
    fact_df = factdetails_df.groupby(fact_cols)\
                            .agg(count('arrivaldate_id').alias('visitorcount'))
    fact_df.persist()
    logger.info("Aggregated data to create fact table")
    return fact_df

def check_data_quality(spark, i94_df, pre_fact_df, fact_df, logger):
    """Perform a small number of data sanity checks"""
    i94_rowcount = i94_df.count()
    fact_rowcount = fact_df.count()
    sumvisitorcount = fact_df.groupBy().sum('visitorcount').collect()[0][0]
    size_reduction = 100*(i94_rowcount-fact_rowcount)/i94_rowcount
    logger.info("Reduction in row count from raw to aggregated fact table is {:.2f}%"\
                .format(size_reduction))
    assert i94_rowcount == sumvisitorcount, (("The row count has changed during processing: " +
                                        "initial rowcount={}, final sum of counts={}")
                                        .format(i94_rowcount, sumvisitorcount))

    dur15to21_sum = fact_df.filter(fact_df['visitduration_id'] == 5)\
                            .groupBy().sum('visitorcount').collect()[0][0]
    dur15to21_rowcount = pre_fact_df.filter((pre_fact_df.duration_days >= 15) &
                                            (pre_fact_df.duration_days <=21)).count()
    assert dur15to21_rowcount == dur15to21_sum, (("The row count has changed during processing: " +
                                            "duration 15 to 21 days rowcount={}, " +
                                            "sum of visitorcounts={}")
                                            .format(dur15to21_rowcount, dur15to21_sum ))

    age0to1_sum = fact_df.filter(fact_df['age_id'] == 1).groupBy().sum('visitorcount').collect()[0][0]
    age0to1_rowcount = pre_fact_df.filter((pre_fact_df.i94bir >= 0) &
                                            (pre_fact_df.i94bir <=1)).count()
    assert dur15to21_rowcount == dur15to21_sum, (("The row count has changed during processing: " +
                                                "age 0 to 1 rowcount={}, " +
                                                "sum of visitorcounts={}")
                                                .format(age0to1_rowcount, age0to1_sum ))
    logger.info("Completed teeny tiny number of data quality checks")


def write_usvisitors_fact(spark, data_path, file_path, df, logger):
    """Write the usvisitor fact to parquet format and append to existing data."""
    out_path = os.path.join(data_path, file_path, 'visit_fact')
    df.coalesce(1)\
    .write\
    .mode('append')\
    .partitionBy('arrivalstate_abbr')\
    .parquet(out_path)

    logger.info("Parquet format appended to {}".format(out_path))


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

    dest_dir='analytics_data/us_visitors'
    log_dir= 'pipeline_logs/us_visitors'

    date_df = get_date_dimension(spark, year, month, logger)
    write_date_dimension(spark, data_path, dest_dir,
                        date_df, logger)

    i94_df = get_i94_df(spark, data_path, year, month, logger)

    port_df = get_port_df(spark, data_path, logger)
    ctry_df = get_ctry_df(spark, data_path, logger)
    dest_df = get_dest_df(spark, data_path, logger)
    mode_df = spark.createDataFrame([(1,'Air'),
                                     (2, 'Sea'),
                                     (3, 'Land'),
                                     (9, 'Not reported'),
                                     (-1, 'unknown')],
                                    ['mode_code', 'mode'] )
    visa_df = spark.createDataFrame([(1,'Business'),
                                     (2, 'Pleasure'),
                                     (3, 'Student'),
                                     (-1, 'unknown')],
                                    ['purpose_code', 'purpose'] )

    i94merge_df = merge_i94_lookup(spark, i94_df, port_df, ctry_df, dest_df,
                                    mode_df, visa_df, logger)
    dur_id_df = add_duration_id(spark, i94merge_df, logger)
    age_id_df = add_age_id(spark, dur_id_df, logger)

    write_rowsandnulls(spark, data_path, log_dir, year, month,
                        age_id_df, logger)
    write_intfield_summary(spark, data_path, log_dir, year, month,
                        age_id_df, logger)
    write_stringfield_summary(spark, data_path, log_dir, year, month,
                        age_id_df, logger)

    fact_df = get_fact(spark, age_id_df, logger)
    check_data_quality(spark, i94_df, age_id_df, fact_df, logger)

    write_usvisitors_fact(spark, data_path, dest_dir, fact_df, logger)

    logger.info("Finished {}. Stopping spark.".format(app_name))
    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1:])
