from helpers.aws_helper import CityAwsHelper
import os 
import logging
import pandas as pd
import geopandas as gpd
import datacompy
import h3
import time

start_time = time.time()

logging.basicConfig(format='%(asctime)s %(message)s',filename='5_further_transformation.log', encoding='utf-8', level=logging.INFO)

aws_config  = {'access_key': 'AKIAYH57YDEWMHW2ESH2',
                'secret_key': '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',
                'region': 'af-south-1'}

cah = CityAwsHelper(aws_config)

if __name__ == '__main__':
    logging.info('------------------------------------------------------------')
    logging.info('Activity 5: Further transforming datasets from AWS S3 bucket')
    logging.info('------------------------------------------------------------')

    #config
    bucket = 'cct-ds-code-challenge-input-data'
    target_files = ['sr_hex.csv.gz']
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
            
    df = cah.extract_zipped_csv(dowload_path, 'sr_hex.csv.gz')

    #find bellville south centroid 
    
        