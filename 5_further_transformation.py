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

    #1. 
    #find bellville south centroid 
    # investigate locations on where this data is located. 
    # start with the city of cape town website


    # Create a filter of existing data to compute location of center to bellville centroid
    #filter all areas that are outside this regoin


    #return dataset of filered arears

    #2. #using the request package and 
    #the URL: https://www.capetown.gov.za/_layouts/OpenDataPortalHandler/DownloadHandler.ashx?DocumentName=Wind_direction_and_speed_2020.ods&DatasetDocument=https%3A%2F%2Fcityapps.capetown.gov.za%2Fsites%2Fopendatacatalog%2FDocuments%2FWind%2FWind_direction_and_speed_2020.ods
    # O will get the response and save the response in a file in the datasets file in this repository
    #I will investigate the file and identify fields to join on and or filter out. 

    #3. 
    
    
        