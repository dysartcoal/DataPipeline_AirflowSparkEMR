from bs4 import BeautifulSoup
import requests
import re
import os
import glob
import sys, getopt
import datetime
import pandas as pd

app_name = 'find_flightno_airports'
def get_flight(airline, fltno):
    apt_pat = '[A-Z]{3,5}'
    depapt = 'unknown'
    arrapt = 'unknown'
    if type(airline) == float or type(fltno) == float:
        return 'unknown', 'unknown'
    try:
        url = 'https://www.flightview.com/flight-tracker/' + airline + '/' + fltno
        r = requests.get(url)
        html_doc = r.text
        soup = BeautifulSoup(html_doc, "lxml")
        for x in soup.find_all('script'):
            data = x.string
            if data:
                m = re.search(f'var sdepapt= \"{apt_pat}\"', data)
                if m:
                    val = re.search(f'{apt_pat}', m.group())
                    if val:
                        depapt = val.group()
                m = re.search(f'var sarrapt= \"{apt_pat}\"', data)
                if m:
                    val = re.search(f'{apt_pat}', m.group())
                    if val:
                        arrapt = val.group()
        if ((depapt != '') & (arrapt != '')):
            return depapt, arrapt
    except Exception as ex:
        print(f'airline = {airline} with type = {type(airline)}')
        print(f'fltno = {fltno} with type = {type(fltno)}')
        print(ex)

    return 'unknown', 'unknown'

def get_unknown_fltno(data_path):
    all_files = glob.glob(os.path.join(data_path, "*/*/unknown_fltno/*.csv"))
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)

    unknown_fltno_df  = pd.concat(li, axis=0, ignore_index=True)
    unknown_fltno_df['fltno'] = unknown_fltno_df['fltno'].apply(str)
    unknown_fltno_df['depapt'] = ''
    unknown_fltno_df['arrapt'] = ''
    unknown_fltno_df['fltno'] = unknown_fltno_df['fltno'].str.lstrip('0')
    return unknown_fltno_df


def write_fltno(data_path, df):
    now = datetime.datetime.now()
    print(now)
    (df.sort_values(['airline', 'fltno'])
        .to_csv(os.path.join(data_path, 'flight', 'flightno_{}.csv'.format(now.strftime("%Y%m%d%H%M"))), header=True)
    )

def move_processed_files(data_path):
    source_files = "*/*/unknown_fltno/*.csv"
    all_files = glob.glob(os.path.join(data_path, source_files))
    processed_path = os.path.join(data_path, "processed")
    if not os.path.exists(processed_path):
        os.mkdir(processed_path)
    for filename in all_files:
        # os.path.basename gets the filename without the path
        new_filename = os.path.join(processed_path, os.path.basename(filename))
        os.rename(filename, new_filename)



def main(argv):
    """Configure the input and output locations and call the processing methods"""

    try:
        opts, args = getopt.getopt(argv,"p:",["path="])
    except getopt.GetoptError:
        logger.info('find_flightno_airports.py -p <data_path>')
        raise Exception('Invalid argument to {}'.format(app_name))
    for opt, arg in opts:
        if opt in ("-p", "--path"):
            data_path = arg

    unknown_fltno_df = get_unknown_fltno(data_path)

    for i, row in enumerate(unknown_fltno_df.iterrows()):
        if (unknown_fltno_df['airline'][i]=='unknown' or
                unknown_fltno_df['fltno'][i]=='unknown'):
            unknown_fltno_df['depapt'][i]='unknown'
            unknown_fltno_df['arrapt'][i]='unknown'
        elif unknown_fltno_df['depapt'][i]=='' or unknown_fltno_df['arrapt'][i]=='':
            unknown_fltno_df['depapt'][i], unknown_fltno_df['arrapt'][i] = get_flight(row[1][0], row[1][1])

    write_fltno(data_path, unknown_fltno_df)
    move_processed_files(data_path)



if __name__ == "__main__":
    main(sys.argv[1:])
