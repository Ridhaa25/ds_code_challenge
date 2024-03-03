from helpers.aws_helper import CityAwsHelper
import os 
import logging
import pandas as pd
import geopandas as gpd
import datacompy
import h3
import time

start_time = time.time()

logging.basicConfig(format='%(asctime)s %(message)s',filename='2_data_transformation.log', encoding='utf-8', level=logging.INFO)

aws_config  = {'access_key': 'AKIAYH57YDEWMHW2ESH2',
                'secret_key': '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',
                'region': 'af-south-1'}

cah = CityAwsHelper(aws_config)


if __name__ == '__main__':
    logging.info('----------------------------------------------------')
    logging.info('Activity 2: Transforming datasets from AWS S3 bucket')
    logging.info('----------------------------------------------------')

    #config
    bucket = 'cct-ds-code-challenge-input-data'
    target_files = ['city-hex-polygons-8.geojson','sr.csv.gz','sr_hex.csv.gz']
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
            logging.info('the files exist already, no new file downloaded')
            
    df = cah.extract_zipped_csv(dowload_path, 'sr.csv.gz')

    if 'latitude' in df.columns and 'longitude' in df.columns:       
        df[['latitude', 'longitude']] = df[['latitude', 'longitude']].fillna(0)

    df['h3_level8_index'] = df.apply(lambda row: h3.geo_to_h3(row['latitude'], row['longitude'], 8), axis = 1)

    df.loc[df[df['latitude']== 0].index, 'h3_level8_index'] = '0'

    #validating the SR dataframe
    validation_df = cah.extract_zipped_csv(dowload_path, 'sr_hex.csv.gz')

    if 'latitude' in validation_df.columns and 'longitude' in validation_df.columns:       
        validation_df[['latitude', 'longitude']] = validation_df[['latitude', 'longitude']].fillna(0)

    match_indicator = df.equals(validation_df)

    print('Dataframe validated: ', match_indicator)
    logging.info(f'Dataframe validated: {match_indicator}')

    compare = datacompy.Compare(df, validation_df, on_index=True)     
    logging.info(compare.report())

    logging.info('Read in polygon 8 file')
    hex_filepath = os.path.join(dowload_path, 'city-hex-polygons-8.geojson')
    pologon_8_set = gpd.read_file(hex_filepath, driver='GeoJSON')    

    merged = df.merge(pologon_8_set, left_on=['h3_level8_index'], right_on= ['index'], how='left', indicator=True)

    failed_merges = merged[(merged['_merge'] == 'left_only') & (merged['latitude'] != 0)]
    successful_merge = merged[merged['_merge'] == 'both']

    #Error threshold 
    #It was found that 3 records did not merge successfully. With the total dataset being 
    #approximately 900K a threshould of 0.5% would equate to approximately 4700 records 
    #Therefore, given the quality of the datasets and join, this is well within an allowable
    #error margin. 
     
    threshold = 0.5
    logging.info(f'Script error margin: {threshold}')

    number_of_failed_merges = len(failed_merges)
    number_of_successful_merges = len(successful_merge)

    error_merge_rate = number_of_failed_merges/number_of_successful_merges

    logging.info(f'number of failed merges are: {number_of_failed_merges}')
    logging.info(f'number of successful merges are: {number_of_successful_merges}')
    logging.info(f'join error rates are: {error_merge_rate}')

    if error_merge_rate > threshold:
            end_time = time.time()            
            logging.error(f"Error rate: ({error_merge_rate}) is higher than the threshold ({threshold})")
            logging.info(f'Total execution time: {end_time - start_time}')
            raise ValueError(f"Error rate: ({error_merge_rate}) is higher than the threshold ({threshold})")

    end_time = time.time()
    logging.info(f'script total execution time: {end_time - start_time}')
    logging.info('----------------------------------------------------')