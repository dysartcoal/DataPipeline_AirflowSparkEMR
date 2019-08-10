import os
import re

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.types import *

if __name__ == "__main__":

    spark = SparkSession\
    .builder\
    .appName("etl_i94")\
    .getOrCreate()

    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info("pyspark script logger initialized")

    #data_path = "s3n://dysartcoal-dend-uswest2/capstone_etl/data/"
    data_path = os.environ.get('HOME') + "/src/python/Udacity/CapstoneProject/prep/"

    imm_data = os.path.join(data_path, "test_data", "immigration-sample.csv")
    imm_df = spark.read.csv(imm_data, header=True)
    imm_df_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res',
                  'i94port', 'i94mode', 'i94addr', 'i94bir', 'i94visa', 'count',
                 'biryear', 'gender', 'airline', 'fltno', 'visatype']

    for col in ['i94yr', 'i94mon', 'i94cit', 'i94res','i94mode',
                'i94bir', 'i94visa', 'count', 'biryear']:
                imm_df = imm_df.withColumn(col, imm_df[col].cast(IntegerType()))
    imm_df = imm_df.select(imm_df_cols)
    imm_df.persist()
    LOGGER.info("imm_df row count: {}".format(imm_df.count()))

    port_data = os.path.join(data_path, 'lookup_data', 'i94port_codes.csv')
    port_df = spark.read.csv(port_data, sep="|", header=True)
    port_df.persist()
    LOGGER.info("port_df row count: {}".format(port_df.count()))

    ctry_data = os.path.join(data_path, 'lookup_data', 'i94cit_i94res_codes.csv')
    ctry_df = spark.read.csv(ctry_data, sep='|', header=True)
    ctry_df = ctry_df.withColumn('code', ctry_df['code'].cast(IntegerType()))
    ctry_df.persist()
    LOGGER.info("ctry_df row count: {}".format(ctry_df.count()))


    fltno_data = os.path.join(data_path, 'analytics_data', 'flight', 'flightno.csv')
    fltno_cols = ['airline', 'fltno', 'dep_airport', 'arr_airport']
    fltno_df = spark.createDataFrame([('', '', '', '')], fltno_cols)
    try:
        fltno_df = spark.read.csv(fltno_data, header=True)
    except:
        pass
    fltno_df.persist()
    LOGGER.info("fltno_df row count: {}".format(fltno_df.count()))


    merge_df = (imm_df.join(port_df, imm_df.i94port==port_df.code, how='left')
            .drop(port_df.code)
            .join(ctry_df, imm_df.i94cit==ctry_df.code, how='left')
            .drop(ctry_df.code).withColumnRenamed('i94ctry', 'citizen_ctry')
            .join(ctry_df, imm_df.i94res==ctry_df.code, how='left')
            .drop(ctry_df.code).withColumnRenamed('i94ctry', 'resident_ctry')
            .join(fltno_df, (imm_df.airline==fltno_df.airline) & (imm_df.fltno == fltno_df.fltno), how='left')
            .drop(fltno_df.airline).drop(fltno_df.fltno)
           )

    merge_df_cols = ['cicid', 'i94yr', 'i94mon',
                    'citizen_ctry', 'resident_ctry',
                    'i94port_raw', 'i94port_state', 'i94port_bps', 'i94port_city',
                    'i94mode', 'i94addr', 'i94visa', 'visatype',
                    'i94bir', 'biryear', 'gender',
                    'airline', 'fltno'
                    ]
    merge_df = merge_df.select(merge_df_cols)

    i94_data = os.path.join(data_path, 'analytics_data', 'i94')
    merge_df.write.parquet(i94_data, mode='overwrite', partitionBy=['i94yr', 'i94mon'])


    unknown_fltno_df = (merge_df.filter(merge_df.depapt.isNull())
                        .filter(merge_df.airline.isNull())
                        .filter(merge_df.fltno.isNull())
                        .select(['airline', 'fltno'])
                        .distinct())
    unknown_fltno_data = os.path.join(data_path, 'staging_data','unknown_fltno')
    unknown_fltno_df.coalesce(1).write.csv(unknown_fltno_data, mode='overwrite', header=True)

    spark.stop()
