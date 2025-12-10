import pytest
from unittest.mock import patch, MagicMock
import json
import uuid
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from datetime import datetime
from infra.functions.validatie.s3_functions import *

# Mock the boto3 client and its send_message method
@patch('boto3.client')
def test_publish_to_sqs(mock_boto3_client):
    # Setup the mock
    mock_sqs_client = MagicMock()
    mock_boto3_client.return_value = mock_sqs_client

    mock_response = {
        'MessageId': 'test_message_id',
        'MD5OfMessageBody': 'test_md5',
        'ResponseMetadata': {
            'RequestId': 'test_request_id'
        }        
    }

    mock_sqs_client.send_message.return_value = mock_response

    # Test parameters
    queue_url = "https://sqs.eu-west-1.amazonaws.com/123456789012/test_queue.fifo"
    message_body = {"key": "value"}
    message_attributes = {"attribute1": {"StringValue": "value1", "DataType": "String"}}
    message_group_id = "test_group_id"
    deduplication_id = str(uuid.uuid4())

    # Call the function
    response = publish_to_sqs(queue_url, message_body, message_attributes, message_group_id, deduplication_id)
    
    # Check if the response is as expected
    assert response == mock_response

    # Check if send_massage was called with the correct parameters
    mock_sqs_client.send_message.assert_called_once_with(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message_body),
        MessageAttributes=message_attributes,
        MessageGroupId=message_group_id,
        MessageDeduplicationId=deduplication_id
    )

@patch('boto3.client')
def test_publish_to_sqs_fifo_queue_without_group_id(mock_boto3_client):
    # Setup the mock
    mock_sqs_client = MagicMock()
    mock_boto3_client.return_value = mock_sqs_client

    # Test parameters for a FIFO queue without message_group_id
    queue_url = "https://sqs.eu-west-1.amazonaws.com/123456789012/test_queue.fifo"
    message_body = {"key": "value"}

    # Check if ValueError is raised when message_group_id is not provided for a FIFO queue
    with pytest.raises(ValueError, match="MessageGroupId is required for FIFO queues."):
        publish_to_sqs(queue_url, message_body)

@patch('boto3.client')
def test_publish_to_sqs_with_string_message_body(mock_boto3_client):
    # Setup the mock
    mock_sqs_client = MagicMock()
    mock_boto3_client.return_value = mock_sqs_client

    # Mock the response from send_message
    mock_response = {
        'MessageId': 'test_message_id',
        'MD5OfMessageBody': 'test_md5',
        'ResponseMetadata': {
            'RequestId': 'test_request_id'
        }
    }
    mock_sqs_client.send_message.return_value = mock_response

    # Test parameters with string message_body
    queue_url = "https://sqs.eu-west-1.amazonaws.com/123456789012/test_queue"
    message_body = "string_message"

    # Call the function
    response = publish_to_sqs(queue_url, message_body)

    # Check if the response is as expected
    assert response == mock_response

    # Check if send_message was called with the correct parameters
    mock_sqs_client.send_message.assert_called_once_with(
        QueueUrl=queue_url,
        MessageBody=message_body,
        MessageAttributes={}
    )

# Mock the boto3 client and its download_file method
@patch('boto3.client')
def test_download_file_from_s3_success(mock_boto3_client):
    # Setup the mock
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client

    # Test parameters
    bucket_name = "test-bucket"
    s3_file_key = "test-file.txt"
    local_file_path = "/tmp/test-file.txt"

    # Call the function
    download_file_from_s3(bucket_name, s3_file_key, local_file_path)

    # Check if download_file was called with the correct parameters
    mock_s3_client.download_file.assert_called_once_with(bucket_name, s3_file_key, local_file_path)

@patch('boto3.client')
def test_download_file_from_s3_failure(mock_boto3_client):
    # Setup the mock to raise an exception
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    mock_s3_client.download_file.side_effect = Exception("Download failed")

    # Test parameters
    bucket_name = "test-bucket"
    s3_file_key = "test-file.txt"
    local_file_path = "/tmp/test-file.txt"

    # Call the function and check if it handles the exception
    download_file_from_s3(bucket_name, s3_file_key, local_file_path)

    # Check if download_file was called with the correct parameters
    mock_s3_client.download_file.assert_called_once_with(bucket_name, s3_file_key, local_file_path)

@patch('boto3.client')
def test_upload_file_to_s3_success(mock_boto3_client):
    # Setup the mock
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client

    # Test parameters
    file_name = "/tmp/test-file.txt"
    bucket_name = "test-bucket"
    s3_file_key = "test-file.txt"

    # Call the function
    result = upload_file_to_s3(file_name, bucket_name, s3_file_key)

    # Check if the function returned True
    assert result is True

    # Check if upload_file was called with the correct parameters
    mock_s3_client.upload_file.assert_called_once_with(file_name, bucket_name, s3_file_key)

@patch('boto3.client')
def test_upload_file_to_s3_file_not_found(mock_boto3_client):
    # Setup the mock to raise a FileNotFoundError
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    mock_s3_client.upload_file.side_effect = FileNotFoundError("File not found")

    # Test parameters
    file_name = "/tmp/non-existent-file.txt"
    bucket_name = "test-bucket"
    s3_file_key = "non-existent-file.txt"

    # Call the function
    result = upload_file_to_s3(file_name, bucket_name, s3_file_key)

    # Check if the function returned False
    assert result is False

    # Check if upload_file was called with the correct parameters
    mock_s3_client.upload_file.assert_called_once_with(file_name, bucket_name, s3_file_key)

@patch('boto3.client')
def test_upload_file_to_s3_no_credentials(mock_boto3_client):
    # Setup the mock to raise a ClientError simulating NoCredentialsError
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    mock_s3_client.upload_file.side_effect = ClientError(
        error_response={'Error': {'Code': 'NoCredentialsError', 'Message': 'Credentials not available'}},
        operation_name='upload_file'
    )

    # Test parameters
    file_name = "/tmp/test-file.txt"
    bucket_name = "test-bucket"
    s3_file_key = "test-file.txt"

    # Call the function
    result = upload_file_to_s3(file_name, bucket_name, s3_file_key)

    # Check if the function returned False
    assert result is False

    # Check if upload_file was called with the correct parameters
    mock_s3_client.upload_file.assert_called_once_with(file_name, bucket_name, s3_file_key)

@patch('boto3.client')
def test_upload_file_to_s3_generic_error(mock_boto3_client):
    # Setup the mock to raise a generic Exception
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    mock_s3_client.upload_file.side_effect = Exception("An error occurred")

    # Test parameters
    file_name = "/tmp/test-file.txt"
    bucket_name = "test-bucket"
    s3_file_key = "test-file.txt"

    # Call the function
    result = upload_file_to_s3(file_name, bucket_name, s3_file_key)

    # Check if the function returned False
    assert result is False

    # Check if upload_file was called with the correct parameters
    mock_s3_client.upload_file.assert_called_once_with(file_name, bucket_name, s3_file_key)

@patch('boto3.client')
def test_delete_file_from_s3_success(mock_boto3_client):
    # Setup the mock
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client

    # Test parameters
    bucket_name = "test-bucket"
    s3_file_key = "test-file.txt"

    # Call the function
    result = delete_file_from_s3(bucket_name, s3_file_key)

    # Check if the function returned True
    assert result is True

    # Check if delete_object was called with the correct parameters
    mock_s3_client.delete_object.assert_called_once_with(Bucket=bucket_name, Key=s3_file_key)

@patch('boto3.client')
def test_delete_file_from_s3_file_not_found(mock_boto3_client):
    # Setup the mock to raise a ClientError simulating FileNotFoundError
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    mock_s3_client.delete_object.side_effect = ClientError(
        error_response={'Error': {'Code': '404', 'Message': 'Not Found'}},
        operation_name='delete_object'
    )

    # Test parameters
    bucket_name = "test-bucket"
    s3_file_key = "non-existent-file.txt"

    # Call the function
    result = delete_file_from_s3(bucket_name, s3_file_key)

    # Check if the function returned True
    assert result is True

    # Check if delete_object was called with the correct parameters
    mock_s3_client.delete_object.assert_called_once_with(Bucket=bucket_name, Key=s3_file_key)

@patch('boto3.client')
def test_delete_file_from_s3_no_credentials(mock_boto3_client):
    # Setup the mock to raise a ClientError simulating NoCredentialsError
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    mock_s3_client.delete_object.side_effect = ClientError(
        error_response={'Error': {'Code': 'NoCredentialsError', 'Message': 'Credentials not available'}},
        operation_name='delete_object'
    )

    # Test parameters
    bucket_name = "test-bucket"
    s3_file_key = "test-file.txt"

    # Call the function
    result = delete_file_from_s3(bucket_name, s3_file_key)

    # Check if the function returned False
    assert result is False

    # Check if delete_object was called with the correct parameters
    mock_s3_client.delete_object.assert_called_once_with(Bucket=bucket_name, Key=s3_file_key)

@patch('boto3.client')
def test_delete_file_from_s3_generic_error(mock_boto3_client):
    # Setup the mock to raise a generic Exception
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    mock_s3_client.delete_object.side_effect = Exception("An error occurred")

    # Test parameters
    bucket_name = "test-bucket"
    s3_file_key = "test-file.txt"

    # Call the function
    result = delete_file_from_s3(bucket_name, s3_file_key)

    # Check if the function returned False
    assert result is False

    # Check if delete_object was called with the correct parameters
    mock_s3_client.delete_object.assert_called_once_with(Bucket=bucket_name, Key=s3_file_key)

# Sample DataFrame for testing
sample_df = pd.DataFrame({
    'krmcriterium': ['test_criterion']
})

# Mock the boto3 client and its methods
@patch('boto3.client')
def test_report_databundle_update_existing(mock_boto3_client):
    # Setup the mock
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client

    # Sample CSV data for testing
    sample_csv_data = "databundelcode;krmcriterium;last_updated;status\npackage1;criterion1;2023-01-01 00:00:00;state1"

    # Mock the response from get_object
    mock_response = {
        'Body': MagicMock()
    }
    mock_response['Body'].read.return_value = sample_csv_data.encode('utf-8')
    mock_s3_client.get_object.return_value = mock_response

    # Test parameters
    package_name = "package1"
    state = "new_state"

    # Call the function
    report_databundle(sample_df, package_name, state)

    # Check if get_object was called with the correct parameters
    mock_s3_client.get_object.assert_called_once_with(Bucket="krm-validatie-data-dev", Key='rapportages/akkoorddata.csv')

    # Check if put_object was called to update the CSV
    mock_s3_client.put_object.assert_called_once()

    # Check if the updated CSV contains the new state
    _, kwargs = mock_s3_client.put_object.call_args
    updated_csv = kwargs['Body']
    assert "new_state" in updated_csv

@patch('boto3.client')
def test_report_databundle_append_new(mock_boto3_client):
    # Setup the mock
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client

    # Sample CSV data for testing
    sample_csv_data = "databundelcode;krmcriterium;last_updated;status\npackage1;criterion1;2023-01-01 00:00:00;state1"

    # Mock the response from get_object
    mock_response = {
        'Body': MagicMock()
    }
    mock_response['Body'].read.return_value = sample_csv_data.encode('utf-8')
    mock_s3_client.get_object.return_value = mock_response

    # Test parameters
    package_name = "package2"
    state = "new_state"

    # Call the function
    report_databundle(sample_df, package_name, state)

    # Check if get_object was called with the correct parameters
    mock_s3_client.get_object.assert_called_once_with(Bucket="krm-validatie-data-prod", Key='rapportages/akkoorddata.csv')

    # Check if put_object was called to update the CSV
    mock_s3_client.put_object.assert_called_once()

    # Check if the updated CSV contains the new package
    _, kwargs = mock_s3_client.put_object.call_args
    updated_csv = kwargs['Body']
    assert "package2" in updated_csv

if __name__ == "__main__":
    pytest.main([__file__])