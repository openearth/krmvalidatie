from datetime import datetime
from io import StringIO
import uuid
import boto3
import json

from botocore.exceptions import NoCredentialsError, ClientError
import pandas as pd

def publish_to_sqs(queue_url, message_body, message_attributes=None, message_group_id=None, deduplication_id=str(uuid.uuid4())):
    """
    Publish a message to an SQS queue.
    
    :param queue_url: The URL of the SQS queue.
    :param message_body: The message body (dict or string).
    :param message_attributes: Optional. Dictionary of message attributes.
    :param message_group_id: Required for FIFO queues. The group ID for the message.
    :param deduplication_id: Optional. Unique deduplication ID for the message (FIFO queues).
    :return: The response from the SQS send_message call.
    """
    # Initialize SQS client
    sqs = boto3.client('sqs')
    
    # Ensure message_body is a string
    if isinstance(message_body, dict):
        message_body = json.dumps(message_body)
    
    try:
        # Prepare request parameters
        params = {
            'QueueUrl': queue_url,
            'MessageBody': message_body,
            'MessageAttributes': message_attributes or {}
        }
        
        # Include FIFO-specific parameters if applicable
        if 'fifo' in queue_url.lower():
            if not message_group_id:
                raise ValueError("MessageGroupId is required for FIFO queues.")
            params['MessageGroupId'] = message_group_id
            if deduplication_id:
                params['MessageDeduplicationId'] = deduplication_id
        
        response = sqs.send_message(**params)
        print(f"Message sent to SQS queue. Message ID: {response['MessageId']}")
        return response
    except Exception as e:
        print(f"Failed to send message to SQS: {str(e)}")
        raise

def download_file_from_s3(bucket_name, s3_file_key, local_file_path):
    """
    Download a file from an S3 bucket to local storage.

    :param bucket_name: The name of the S3 bucket.
    :param s3_file_key: The key (path) of the file in the S3 bucket.
    :param local_file_path: The local path where the file will be saved.
    """
    # Create an S3 client
    s3 = boto3.client('s3')

    try:
        # Download the file
        s3.download_file(bucket_name, s3_file_key, local_file_path)
        print(f"File {s3_file_key} has been downloaded to {local_file_path}.")
    except Exception as e:
        print(f"Error downloading file: {e}")

def upload_file_to_s3(file_name, bucket_name, s3_file_key):
    """
    Uploads a local file to an S3 bucket.

    :param file_name: Path to the local file
    :param bucket_name: Name of the S3 bucket
    :param s3_file_key: S3 object key (name of the file in S3)
    :return: True if file was uploaded, else False
    """
    # Create an S3 client
    s3 = boto3.client('s3')
    
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

def delete_file_from_s3(bucket_name, s3_file_key):
    """
    Deletes a file from an S3 bucket.

    :param bucket_name: Name of the S3 bucket.
    :param file_key: The key (path) of the file to delete.
    :param region_name: The AWS region where the bucket is located.
    :return: True if the file was deleted successfully, False otherwise.
    """
    # Initialize a session using Amazon S3
    s3 = boto3.client('s3')

    try:
        # Delete the file
        s3.delete_object(Bucket=bucket_name, Key=s3_file_key)
        print(f"File {s3_file_key} deleted successfully from bucket {bucket_name}.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"The file {s3_file_key} does not exist in the bucket {bucket_name}.")
            return True
        else:
            print(f"An error occurred: {e}")
            return False
    except NoCredentialsError:
        print("Credentials not available.")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
       
def report_databundle(df, package_name, state):

    s3 = boto3.client('s3')
    
    bucket_name = "krm-validatie-data-prod"

    response = s3.get_object(Bucket=bucket_name, Key='rapportages/akkoorddata.csv')
    csv_data = response['Body'].read().decode('utf-8')
    
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    row = { 
            'databundelcode': package_name,
            'krmcriterium': df['krmcriterium'].values[0],
            'last_updated': current_datetime,
            'status': state
           }

    # Step 2: Load the CSV data into a DataFrame
    val = pd.read_csv(StringIO(csv_data), sep=';')

    if package_name in val['databundelcode'].values:
        # Update the existing record
        val.loc[val['databundelcode'] == package_name, ['krmcriterium', 'status', 'last_updated']] = row['krmcriterium'], row['status'], row['last_updated']
    else:
        # Append the new record
        new_record_df = pd.DataFrame([row])
        val = pd.concat([val, new_record_df], ignore_index=True)

    # Step 4: Write the updated DataFrame back to a CSV format
    csv_buffer = StringIO()
    val.to_csv(csv_buffer, index=False, sep=';')

    # Step 5: Upload the updated CSV back to S3
    s3.put_object(Bucket=bucket_name, Key='rapportages/akkoorddata.csv', Body=csv_buffer.getvalue())
