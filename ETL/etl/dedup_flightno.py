import os
import glob
import sys, getopt
import datetime
import pandas as pd

app_name = 'dedup_flightno.py'

def get_flights(data_path, analytics_path):
    source_files = "flight/*.csv"
    all_files = glob.glob(os.path.join(data_path, source_files ))
    li = []

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)

    curr_file = glob.glob(os.path.join(analytics_path, "flight/flightno.csv" ))
    for filename in curr_file:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)

    if len(li) == 0:
        return pd.DataFrame({'A' : []})

    fltno_df  = pd.concat(li, axis=0, ignore_index=True)
    fltno_df = fltno_df[['airline', 'fltno', 'depapt', 'arrapt']]
    fltno_df['fltno']= fltno_df['fltno'].apply(str)
    fltno_df = fltno_df.drop_duplicates()
    return fltno_df


def dedup_flights(df):
    # Find all of the flight numbers without duplicates first
    df_grp = df.groupby(['airline', 'fltno']).size()
    flat_df_grp = df_grp.to_frame(name='size').reset_index()
    good_df = flat_df_grp.loc[flat_df_grp['size'] == 1, :]
    good_airport_df = (good_df.merge(df,
                                left_on=['airline', 'fltno'],
                                right_on=['airline', 'fltno'])
                            .loc[:, ['airline', 'fltno', 'depapt', 'arrapt']])
    print('Num rows of good_airport_df = {}'.format(good_airport_df.shape[0]))

    # Resolve the duplicates so that the good values
    # can be appended to the good_df list
    dup_df = flat_df_grp.loc[flat_df_grp['size'] > 1, :]
    dup_airport_df = (dup_df.merge(df,
                                    left_on=['airline', 'fltno'],
                                    right_on=['airline', 'fltno'])
                            .loc[:, ['airline', 'fltno', 'depapt', 'arrapt']])

    # Remove all the duplicates where the values for dep and arr airport are 'unknown'
    dup_valid_airport_df = (dup_airport_df.loc[(dup_airport_df['depapt'] != 'unknown')
                                            & (dup_airport_df['arrapt'] != 'unknown'), :])

    # Then check that there are no duplicates on the valid dep and arr airports
    dup_valid_airport_grp_df =dup_valid_airport_df.groupby(['airline', 'fltno']).size()
    flat_dup_valid_airport_grp_df  = dup_valid_airport_grp_df.to_frame(name='size').reset_index()
    good_dup_df = flat_dup_valid_airport_grp_df.loc[flat_dup_valid_airport_grp_df['size'] == 1, :]

    additional_airport_df = (dup_valid_airport_df
                 .merge(good_dup_df, left_on=['airline', 'fltno'],
                                    right_on=['airline', 'fltno'])
                    .loc[:, ['airline', 'fltno', 'depapt', 'arrapt']]
                )
    full_df = good_airport_df.append(additional_airport_df)
    print('Num rows of full_df = {}'.format(full_df.shape[0]))
    return full_df


def write_flights(analytics_path, df):
    df.sort_values(['airline', 'fltno'])\
        .to_csv(os.path.join(analytics_path, 'flight', 'flightno.csv'), header=True)

def move_processed_files(data_path):
    source_files = "flight/*.csv"
    all_files = glob.glob(os.path.join(data_path, source_files))
    processed_path = os.path.join(data_path, "processed")
    flight_path = os.path.join(processed_path, "flight")
    if not os.path.exists(processed_path):
        os.mkdir(processed_path)
    if not os.path.exists(flight_path):
        os.mkdir(flight_path)
    for filename in all_files:
        # os.path.basename gets the filename without the path
        new_filename = os.path.join(flight_path, os.path.basename(filename))
        os.rename(filename, new_filename)


def main(argv):
    """Configure the input and output locations and call the processing methods"""

    try:
        opts, args = getopt.getopt(argv,"s:a:",["staging=","analytics="])
    except getopt.GetoptError:
        logger.info('{} -s <staging_data_path>  -a <analytics_data_path>'.format(app_name))
        raise Exception('Invalid argument to {}'.format(app_name))
    for opt, arg in opts:
        if opt in ("-s", "--staging"):
            staging_path = arg
        elif opt in ("-a", "--analytics"):
            analytics_path = arg

    df = get_flights(staging_path, analytics_path)
    final_df = dedup_flights(df)
    write_flights(analytics_path, final_df)
    move_processed_files(staging_path)



if __name__ == "__main__":
    main(sys.argv[1:])
