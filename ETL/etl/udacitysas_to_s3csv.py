from io import StringIO
from string import Template
import pandas as pd
import boto3
import configparser
import os

# Set up for copy to Amazon S3
config = configparser.ConfigParser()
config.read('cap.cfg')

KEY                    = config.get('AWS','AWS_ACCESS_KEY_ID')
SECRET                 = config.get('AWS','AWS_SECRET_ACCESS_KEY')
DWH_CLUSTER_REGION     = config.get("DWH","DWH_CLUSTER_REGION")

s3 = boto3.resource('s3',
                  region_name=DWH_CLUSTER_REGION,
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET
                  )

# Input and output files
filename_template = Template('i94_${mmm}16_sub.sas7bdat')
outname_template = Template('i94_${mmm}16_sub_${index}.csv')
bucket = "dysartcoal-dend-uswest2"
key = "capstone_etl/data/sas_data"

# Local file for recording row counts
local_outpath = './sas_data_csv'
rowcount_file = 'sas_data_rowcounts.csv'
if not os.path.exists(local_outpath):
    os.mkdir(local_outpath)
with open(os.path.join(local_outpath, rowcount_file), 'w') as f:
    f.write('sas_data_year,sas_data_month,sas_data_rowcount\n')


# Write all of the data to S3 as individual csv files for later processing in AWS EMR
month_dict = {'jan':1, 'feb':2, 'mar':3, 'apr':4, 'may':5, 'jun':6, 'jul':7, 'aug':8, 'sep':9, 'oct':10, 'nov':11, 'dec':12}
for month, month_num in month_dict.items():
    sasds = pd.read_sas(os.path.join('../../data/18-83510-I94-Data-2016/',
                                     filename_template.substitute(mmm=month)),
                          chunksize=100000,
                          iterator=True)
    rowcount = 0
    for i,df in enumerate(sasds):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        rowcount += df.shape[0]
        outname = outname_template.substitute(mmm=month, index='{:03d}'.format(i))
        s3.Object(bucket, os.path.join(key, '2016', '{:02d}'.format(month_num), outname))\
            .put(Body=csv_buffer.getvalue())
        csv_buffer.close()
    with open(os.path.join(local_outpath, rowcount_file), 'a') as f:
        f.write(f'2016,{month_num},{rowcount}\n')

# Save the rowcount with the csv data on S3
rowcount_df = pd.read_csv(os.path.join(local_outpath, rowcount_file))
csv_buffer = StringIO()
rowcount_df.to_csv(csv_buffer)
s3.Object(bucket, os.path.join(key, rowcount_file)).put(Body=csv_buffer.getvalue())

        
