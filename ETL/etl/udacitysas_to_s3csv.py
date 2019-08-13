"""
Example:
python udacitysas_to_s3csv.py -y 2016 -m 1 -p ../../data -a cap.cfg -b dysartcoal-dend-uswest2 -k capstone_etl/data/sas_data
"""
from io import StringIO
from string import Template
import pandas as pd
import boto3
import configparser
import os
import sys, getopt

app_name='udacitysas_to_s3csv'

def get_s3_resource(config_filename):
    """Return an s3 resource object"""
    # Set up for copy to Amazon S3
    config = configparser.ConfigParser()
    config.read(config_filename)

    KEY                    = config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET                 = config.get('AWS','AWS_SECRET_ACCESS_KEY')
    DWH_CLUSTER_REGION     = config.get("DWH","DWH_CLUSTER_REGION")

    s3 = boto3.resource('s3',
                      region_name=DWH_CLUSTER_REGION,
                      aws_access_key_id=KEY,
                      aws_secret_access_key=SECRET
                      )
    return s3


def sas_to_s3csv(sas_path, year, month_num, s3, bucket, key):
    """Read the data from SAS files and write to multiple csv files on S3.

    Arguments:
    sas_path -- path to input data
    year -- integer year (4 digit)
    month_num -- integer month
    s3 -- s3 resource object
    bucket -- s3 bucket name
    key -- key name for file to be written.  This will be appended with suitable
    identifiers for the current year and month.
    """

    yr = str(year)[2:]
    filename_template = Template('i94_${mmm}${yy}_sub.sas7bdat')
    outname_template = Template('i94_${mmm}${yy}_sub_${index}.csv')
    rowcount_template = Template('rowcount_udacitysas_${year}_${month}.csv')

    # Write all of the data to S3 as individual csv files of 100K records
    month_dict = {1:'jan', 2:'feb', 3:'mar', 4:'apr', 5:'may', 6:'jun', 7:'jul', 8:'aug', 9:'sep', 10:'oct', 11:'nov', 12:'dec'}
    month = month_dict[month_num]
    sasds = pd.read_sas(os.path.join(sas_path, '18-83510-I94-Data-{}/'.format(year),
                                     filename_template.substitute(mmm=month, yy=yr)),
                          chunksize=100000,
                          iterator=True)
    rowcount = 0
    for i,df in enumerate(sasds):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        rowcount += df.shape[0]
        outname = outname_template.substitute(mmm=month, yy=yr, index='{:03d}'.format(i))
        s3.Object(bucket, os.path.join(key, str(year), '{:02d}'.format(month_num), outname))\
            .put(Body=csv_buffer.getvalue())
        csv_buffer.close()

    # Write a row count for data quality checks at a later stage of the pipeline
    csv_buffer = StringIO()
    csv_buffer.write('sas_data_year,sas_data_month,sas_data_rowcount\n')
    csv_buffer.write(f'{year},{month_num},{rowcount}\n')
    rowcount_file = rowcount_template.substitute(month='{:02d}'.format(month_num), year=year)
    s3.Object(bucket, os.path.join(key, rowcount_file))\
        .put(Body=csv_buffer.getvalue())
    csv_buffer.close()


def main(argv):
    """Configure the input and output locations and call the processing methods"""

    try:
        month, year, data_path, config_filename, bucket, key = None, None, None, None, None, None

        opts, args = getopt.getopt(argv,"y:m:p:a:b:k:",
                                    ["year=","month=","path=","awsconfig=","bucket=","key="])
    except getopt.GetoptError:
        print('{}.py -y <year_int> -m <month_int> -p <path_to_data> -a <aws_config_file> -b <s3_bucket> -k <s3_key>'.format(app_name))
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
        elif opt in ("-p", "--path"):
            data_path = arg
        elif opt in ("-a", "--awsconfig"):
            config_filename = arg
        elif opt in ("-b", "--bucket"):
            bucket = arg
        elif opt in ("-k", "--key"):
            key = arg

    print(f'data_path is {data_path}')
    print(f'config_file is {config_filename}')
    print(f'year is {year}')
    print(f'month is {month}')
    print(f'bucket is {bucket}')
    print(f'key is {key}')

    if year==None or month==None or data_path==None or config_filename==None or bucket==None or key==None:
        raise Exception(('Args are missing that are required: '
                        + '{}.py -y <year_int> -m <month_int> -p <path_to_data> '
                        + '-a <aws_config_file> -b <s3_bucket> -k <s3_key>').format(app_name))

    s3 = get_s3_resource(config_filename)
    sas_to_s3csv(data_path, year, month, s3, bucket, key)



if __name__ == "__main__":
    main(sys.argv[1:])
