import pytest
from unittest.mock import patch, MagicMock
import io
import zipfile
import pandas as pd
import boto3
from infra.functions.validatie import extract_csv_from_zip_in_s3

# Sample CSV data for testing
sample_csv_data = "col1;col2\nval1;val2\nval3;val4"

# Create a mock zip file in memory
def create_mock_zip():
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        # Add a CSV file to the zip
        zip_file.writestr('data.csv', sample_csv_data)
        # Add an akkoord.txt file to the zip
        zip_file.writestr('akkoord.txt', 'Akkoord content')
    zip_buffer.seek(0)
    return zip_buffer

# Mock the boto3 client and its get_object method
@patch('boto3.client')
def test_extract_csv_from_zip_in_s3_success(mock_boto3_client):
    # Setup the mock
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client

    # Create a mock zip file
    mock_zip_data = create_mock_zip().read()

    # Mock the response from get_object
    mock_response = {
        'Body': MagicMock()
    }
    mock_response['Body'].read.return_value = mock_zip_data
    mock_s3_client.get_object.return_value = mock_response

    # Test parameters
    bucket_name = "test-bucket"
    zip_file_key = "path/to/zipfile.zip"

    # Call the function
    result_df = extract_csv_from_zip_in_s3(bucket_name, zip_file_key)

    # Check if get_object was called with the correct parameters
    mock_s3_client.get_object.assert_called_once_with(Bucket=bucket_name, Key=zip_file_key)

    # Check if the result is a DataFrame and not empty
    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty

    # Check if the DataFrame contains the expected data
    assert list(result_df.columns) == ['col1', 'col2']
    assert len(result_df) == 2

@patch('boto3.client')
def test_extract_csv_from_zip_in_s3_no_csv(mock_boto3_client):
    # Setup the mock
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client

    # Create a mock zip file without a CSV file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        # Add only an akkoord.txt file to the zip
        zip_file.writestr('akkoord.txt', 'Akkoord content')
    mock_zip_data = zip_buffer.getvalue()

    # Mock the response from get_object
    mock_response = {
        'Body': MagicMock()
    }
    mock_response['Body'].read.return_value = mock_zip_data
    mock_s3_client.get_object.return_value = mock_response

    # Test parameters
    bucket_name = "test-bucket"
    zip_file_key = "path/to/zipfile.zip"

    # Call the function
    result = extract_csv_from_zip_in_s3(bucket_name, zip_file_key)

    # Check if get_object was called with the correct parameters
    mock_s3_client.get_object.assert_called_once_with(Bucket=bucket_name, Key=zip_file_key)

    # Check if the result is None
    assert result is None

if __name__ == "__main__":
    pytest.main([__file__])
