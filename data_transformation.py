from helpers.aws_helper import CityAwsHelper
import os 
import logging
import gzip
import pandas as pd
import geopandas as gpd
from io import StringIO
import h3

logging.basicConfig(format='%(asctime)s %(message)s',filename='Transformation_performance.log', encoding='utf-8', level=logging.INFO)


aws_config  = {'access_key': 'AKIAYH57YDEWMHW2ESH2',
                'secret_key': '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',
                'region': 'af-south-1'}

cah = CityAwsHelper(aws_config)


if __name__ == '__main__':
    logging.info('--------------------------------------------------')
    logging.info('Activity 2: Transforming datasets from AWS S3 bucket')
    logging.info('--------------------------------------------------')

    #config
    bucket = 'cct-ds-code-challenge-input-data'
    target_files = ['city-hex-polygons-8.geojson','sr.csv.gz']
    dowload_path = 'aws_downloads/'

    logging.info('checking if files exist, if not download them')
    for file in target_files:
        filepath = os.path.join(dowload_path, file)

        if not os.path.exists(filepath):
            logging.info(f'Downloading file: {file} from {bucket}')
            cah.download_file_from_bucket(bucket, file, path=dowload_path)
            logging.info(f'{file} downlaoded successfully')
        else:
            logging.info(f'{file} exists in directory {dowload_path}')
            logging.warning('the files exist already')

    logging.warning('Read service records')
    #extact files from directory    
    with gzip.open(dowload_path + 'sr.csv.gz', 'rb') as f:
        Service_requests = f.read().decode('utf-8')

    logging.warning('convert file to dataframe, remove unwanted columns and fillna in lat and long columns')
    df = pd.read_csv(StringIO(Service_requests))    
    df = df.drop(columns=['Unnamed: 0'])
    df[['latitude', 'longitude']] = df[['latitude', 'longitude']].fillna(0)

    df['h3_index'] = df.apply(lambda row: h3.geo_to_h3(row['latitude'], row['longitude'], 8), axis = 1)


    #todo: validate this dataset df with sr_hex.csv.gz



    logging.debug('Read in polygon 8 file')
    hex_filepath = os.path.join(dowload_path, 'city-hex-polygons-8.geojson')
    pologon_8_set = gpd.read_file(hex_filepath, driver='GeoJSON')    

    merged = df.merge(pologon_8_set, left_on=['h3_index'], right_on= ['index'], how='left', indicator=True)

    failed_merges = merged[(merged['_merge'] == 'left_only') & (merged['latitude'] != 0)]

    successful_merge = merged[merged['_merge'] == 'both']

    #error threshold 
    #error rate chosen because the current error rate is x and this
    threshold = 0.5
    number_of_failed_merges = len(failed_merges)
    number_of_successful_merges = len(successful_merge)

    error_merge_rate = number_of_failed_merges/number_of_successful_merges

    if error_merge_rate > threshold:
            raise ValueError(f"Error rate: ({error_merge_rate}) is higher than the threshold ({threshold})")
    
    #write the metrics here,;.
    


    # pologon_8_set= pologon_8_set.rename(columns= {'centroid_lat':'latitude','centroid_lon':'longitude'})
    
    # merged = df.merge(pologon_8_set, on=['latitude', 'longitude'], how='outer', indicator=True)

    # merged['_merge'].unique()
    # see = merged[merged['_merge'] == 'right_only']
    # see1 = merged[merged['_merge'] == 'left_only']

# value = df.loc[1, 'latitude']
# value = pologon_8_set.loc[1, 'latitude']





#read in the city hex file into geojson

#get serive requests 
#extract service requests and see contents

#check lat and longitude to see what it looks like and fillna with 0 

#failed to join - add it in here. 
#choose a threshold 

#raise if error more than that

# use sr_hex to compare results

#log the time for all the operations

