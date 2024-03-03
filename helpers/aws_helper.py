import boto3
import pandas as pd
import logging
import geopandas as gpd
from shapely.geometry import Polygon
import time

class CityAwsHelper:
    def __init__(self, aws_config) -> None:        
        self.s3_client = boto3.client('s3',region_name =aws_config['region'] ,
                          aws_access_key_id=aws_config['access_key'], 
                          aws_secret_access_key=aws_config['secret_key'])

    def get_geojson_df_from_query(self, bucket:str, file:str, query:str): 
        """Downloads and parses a geojson file from s3 bucket

        Args:
            bucket (str): name of s3 bucket
            file (str): name of file to download
            query (str): select * query to extract data from file

        Returns:
            dataframe: resultant dataframe
            Times: times of each execution
        """
        start_time = time.time()
        logging.info(f'start extracting {file} from bucket {bucket}')

        df = self.get_data_from_query(bucket=bucket ,file=file ,query=query)
        
        Extraction_time = time.time()
        logging.info(f'extraction successful, start file parsing')

        extension = self.get_file_exension(file)
        result = self.parse_geojson_dataset(extension=extension, df=df)

        parsing_time = time.time()
        logging.info(f'Parsing successful, returning dataframe')

        times = {'extraction_time':Extraction_time - start_time,
                 'parsing_time':parsing_time - Extraction_time,
                 'total_time':parsing_time - start_time,
                  }
        
        return result, times

    def get_data_from_query(self, bucket:str, file:str, query:str):
        """Generic function to extract data from s3 bucket using a query

        Args:
            bucket (str): name of s3 bucket
            file (str): name of file to download
            query (str): select * query to extract data from file

        Returns:
            dataframe: resultant dataframe
        """
        response = self.s3_client.select_object_content( 
                    Bucket = bucket,
                    Key = file,
                    ExpressionType = 'SQL',
                    Expression = query, 
                    InputSerialization = {"JSON": {"Type": "DOCUMENT"}, "CompressionType": "NONE"},
                    OutputSerialization = {'JSON': {}}
                )

        event_stream = response['Payload']

        rows = ''
        for event in event_stream:
            if 'Records' in event:
                data = event['Records']['Payload'].decode('utf-8')
                rows += data

            elif 'End' in event:
                    print('file download complete')
                    end_event_received = True

        extracted_dataframe = pd.read_json(rows, lines=True)   

        return extracted_dataframe

    @staticmethod
    def parse_geojson_dataset(extension:str, df: pd.DataFrame):
        """method to parse geojson file. This ensures that the files are in the correct 
        format
        

        Args:
            extension (str): file extension extracted
            df (pd.DataFrame): input dataframe

        Returns:
            _type_: _description_
        """

        if extension == 'geojson':

            columns = ['type','properties','geometry']
            assert all(col in df.columns for col in columns), f"Not all columns {columns} are present in the DataFrame"

            df = pd.concat([df.drop(['properties'], axis=1), df['properties'].apply(pd.Series)], axis=1)
            df = df[['index','centroid_lat', 'centroid_lon','geometry']]
            df['geometry'] = df['geometry'].apply(lambda row: Polygon(row['coordinates'][0]))

            parsed_df = gpd.GeoDataFrame(df)
            return parsed_df

        else:
            AssertionError(f'extension {extension} not a supported extension...')
            return False

    @staticmethod
    def get_file_exension(file:str):
        """method to return the file extension

        Args:
            file (str): filename

        Returns:
            string: file extension
        """
        parts = file.split('.')

        if len(parts) > 1:
            return parts[-1]
        else:
            return False
        
    def list_files_in_bucket(self,bucket): 
        """list all objects within a respective s3 bucket

        Args:
            bucket (str): name of s3 bucket

        Returns:
            list: list of objects within bucket
        """
        #TODO: check if the bucket exists
        files = self.s3_client.list_objects(Bucket = bucket)['Contents']
        objects = [file['Key'] for file in files]   

        return objects       
         
    #todo: download file from the storage
    def download_file_from_bucket(self, bucket, file, path:str ='aws_downloads/'):

        files = self.list_files_in_bucket(bucket)

        if file in files:            
            self.s3_client.download_file(bucket, file,path + file)
            return True
        else:
            AssertionError(f'file {file} not in bucket": {bucket}')
            return False