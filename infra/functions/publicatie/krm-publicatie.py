import urllib3
import boto3
from botocore.exceptions import NoCredentialsError
import geopandas as gpd
import pandas as pd
import os
import glob

s3 = boto3.client('s3')

def get_all_files_from_s3(bucket_name, folder_name):
    
    # List all objects in the specified S3 folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f'{folder_name}/')
    
    # Check if the folder is empty
    if 'Contents' not in response:
        return {
            'statusCode': 404,
            'body': 'No files found in the specified S3 folder.'
        }
    
    # Iterate over all files and download them
    for obj in response['Contents']:
        key = obj['Key']

        if (key != folder_name + '/'):
            # Download each file to the Lambda's /tmp directory
            print(key)
            download_path = os.path.join('/tmp', key.split('/')[-1])
            s3.download_file(bucket_name, key, download_path)
            
            # Do something with the downloaded file (if needed)
            print(f"Downloaded {key} to {download_path}")
    
    print("Done")


def upload_file_to_s3(file_name, bucket_name, s3_file_key):
    """
    Uploads a local file to an S3 bucket.

    :param file_name: Path to the local file
    :param bucket_name: Name of the S3 bucket
    :param s3_file_key: S3 object key (name of the file in S3)
    :return: True if file was uploaded, else False
    """
    try:
        s3.upload_file(file_name, bucket_name, s3_file_key)
        print(f"File {file_name} successfully uploaded to {bucket_name}/{s3_file_key}")
        return True
    except FileNotFoundError:
        print(f"File {file_name} was not found.")
        return False
    except NoCredentialsError:
        print("Credentials not available.")
        return False
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False

def merge_geopackages(gpkg_files, output_gpkg, layer_name="krm_actuele_dataset"):
    # Initialize an empty GeoDataFrame
    print(1)
    gdf_list = []
    common_crs = 'EPSG:4258'
    # Loop through each geopackage file and read them
    for gpkg_file in gpkg_files:
        print(gpkg_file)
        gdf = gpd.read_file(gpkg_file)
        # Check the CRS of the current GeoDataFrame
        if gdf.crs != common_crs:
            # Transform the CRS to the common CRS
            gdf = gdf.to_crs(common_crs)

        gdf.columns = [col.lower() for col in gdf.columns]

        gdf_list.append(gdf)

    # Concatenate all GeoDataFrames into one
    merged_gdf = pd.concat(gdf_list, ignore_index=True)

    
    # Ensure the output folder exists
    os.makedirs(os.path.dirname(output_gpkg), exist_ok=True)

    # Save the merged GeoDataFrame to a new GeoPackage file
    merged_gdf.to_file(output_gpkg, layer=layer_name, driver="GPKG")

    print(f"Merged {len(gpkg_files)} GeoPackages into {output_gpkg}")


def lambda_handler(event, context):

    bucket_name = "krm-validatie-data-prod"
    # TODO get latest file in bucket
    subfolder = "geopackages"
    
    try:
        # Grab historic package
        # s3.download_file(bucket_name, historic_key, local_key)
        print("download")
        # Grab new packages
        get_all_files_from_s3(bucket_name, subfolder)
        print("downloaded")
        # Get all .gpkg files from /tmp directory
        geopackage_files = glob.glob("/tmp/*.gpkg")
        #geopackage_files = glob.glob("*.gpkg")
        print(geopackage_files)

        # Specify the output GeoPackage file
        output_geopackage = "/tmp/merged_output.gpkg"
        #output_geopackage = "merged_output.gpkg"

        # Merge all GeoPackages
        merge_geopackages(geopackage_files, output_geopackage)
        
        upload_file_to_s3('/tmp/merged_output.gpkg', 'krm-validatie-data-prod', 'geopackages_history/krm_actuele_dataset_new.gpkg')
        #upload_file_to_s3('merged_output.gpkg', 'krm-validatie-data-prod', 'geopackages_history/krm_actuele_dataset_new.gpkg')

        # Publish new package
        publish_bucket = "krm-validatie-data-prod"
        publish_key = "geopackages_history/krm_actuele_dataset_new.gpkg"
        publish_to_test = True

        for record in event['Records']:
            event_source = record.get('EventSource')
            print(record)
            if event_source == 'aws:sns':
                topic_arn = record['Sns']['TopicArn']
                print(topic_arn)
                
                # Check if 'PublishDataToProd' is in the TopicArn
                if 'PublishDataToProd' in topic_arn:
                    publish_to_test = False
                    break

        url =f'https://marineprojects.openearth.nl/wps?request=Execute&service=WPS&identifier=wps_mp_dataingestion&version=2.0.0&DataInputs=s3_inputs={{"bucketname":"{publish_bucket}","key":"{publish_key}","test":"{publish_to_test}"}}'
        # Send an HTTP GET request to the URL
        print(url)
        http = urllib3.PoolManager()
        response = http.request('GET', url)

        print(response.status)

    except Exception as e:
        print(str(e))
        return {
            'statusCode': 500,
            'message': str(e)
        }
    
#lambda_handler('', '')