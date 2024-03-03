import datacompy
import pandas as pd
import logging
import geopandas as gpd
from helpers.aws_helper import CityAwsHelper
import time

start_time = time.time()

#This would normally be stored in environmental variables.
aws_config  = {'access_key': 'AKIAYH57YDEWMHW2ESH2',
                'secret_key': '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',
                'region': 'af-south-1'}
        
cah = CityAwsHelper(aws_config)

logging.basicConfig(format='%(asctime)s %(message)s',filename='Extraction_performance.log', encoding='utf-8', level=logging.INFO)

if __name__ == '__main__':
    logging.info('--------------------------------------------------')
    logging.info('Activity 1: extracting dataset from AWS S3 bucket')
    logging.info('--------------------------------------------------')

    # #set config here
    bucket = 'cct-ds-code-challenge-input-data'
    object_to_query = 'city-hex-polygons-8-10.geojson'
    resolution = 8
    query = f"SELECT * FROM S3Object[*].features[*] rec where rec.properties.resolution = {resolution}"

    logging.info(f'extraction config is: \nBucket:{bucket}\nFile:{object_to_query}\nQuery:{query}')
                 
    pologon_8_downloaded_set, func_times = cah.get_geojson_df_from_query(bucket=bucket ,file=object_to_query ,query=query)
    pologon_8_downloaded_set = pologon_8_downloaded_set.set_index('index')

    validation_time = time.time()
    pologon_8_validation_set = gpd.read_file('datasets/city-hex-polygons-8.geojson', driver='GeoJSON')
    pologon_8_validation_set = pologon_8_validation_set.set_index('index')

    compare = datacompy.Compare(pologon_8_downloaded_set, pologon_8_validation_set, on_index=True) 
    
    logging.info(compare.report())
    end_time = time.time()
    
    #metrics
    are_equal = pologon_8_downloaded_set.equals(pologon_8_validation_set)
    logging.info(f'dataset match: {are_equal}')

    extaction_time = func_times['extraction_time']
    parsing_time = func_times['parsing_time']
    extract_and_parse_time = func_times['total_time']

    logging.info('--------------------------------------------------')
    #time metrics
    logging.info('Script time metrics')
    logging.info(f'Script total time: {end_time - start_time}')
    logging.info(f'Dataset extract time: {extaction_time}')
    logging.info(f'Dataset parsing time: {parsing_time}')
    logging.info(f'Extract and parsing total time: {extract_and_parse_time}')
    logging.info(f'Validation total time: {end_time - validation_time}')
    logging.info('--------------------------------------------------')

    print('extraction and validation completed, please see results in Extraction_performance.log file')