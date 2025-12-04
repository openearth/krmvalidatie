import pytest
import pandas as pd
import urllib3

from unittest.mock import patch, MagicMock
from io import StringIO
from infra.functions.validatie.github_functions import *

sample_csv_data = "col1;col2\nval1;val2\nval3;val4"

def mock_request(*args, **kwargs):
    # Create a mock response
    response = MagicMock()
    response.data = sample_csv_data.encode('windows-1252')
    response.status = 200
    return response

def mock_failed_request(*args, **kwargs):
    # Create a mock response for a failed request
    response = MagicMock()
    response.data = b""
    response.status = 404
    return response

@patch('urllib3.PoolManager')
def test_get_data_from_github(mock_pool_manager):
    # Setup the mock
    mock_pool_manager.return_value.request = mock_request

    url = 'https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/validatielijst.csv'
    result_df = get_data_from_github(url)

    # Check if DataFrame is not empty
    assert not result_df.empty

    # Check if DataFrame has the expected columns
    assert 'col1' in result_df.columns
    assert 'col2' in result_df.columns
    assert 'new_index' in result_df.columns

    # Check if the new_index column is correctly assigned
    assert list(result_df['new_index']) == [1, 2]

    # Check if the data is correctly read
    assert result_df.loc[0, 'col1'] == 'val1'
    assert result_df.loc[0, 'col2'] == 'val2'
    assert result_df.loc[1, 'col1'] == 'val3'
    assert result_df.loc[1, 'col2'] == 'val4'

@patch('urllib3.PoolManager')
def test_get_data_from_github_failed_request(mock_pool_manager):
    # Setup the mock for a failed request
    mock_pool_manager.return_value.request = mock_failed_request

    # Call the function with a dummy URL
    url = "https://wrongurl.com/data.csv"
    result_df = get_data_from_github(url)

    assert result_df is None

@patch('urllib3.PoolManager')
def test_get_data_from_github_different_encoding(mock_pool_manager):
    # Setup the mock
    mock_pool_manager.return_value.request = mock_request

    # Call the function with a dummy URL and different encoding
    url = "https://righturl.com/data.csv"
    result_df = get_data_from_github(url, encoding='utf-8')

    assert not result_df.empty

@patch('urllib3.PoolManager')
def test_get_data_from_github_different_delimiter(mock_pool_manager):
    # Sample CSV data with a different delimiter
    sample_csv_data_diff_delimiter = "col1,col2\nval1,val2\nval3,val4"

    def mock_request_diff_delimiter(*args, **kwargs):
        # Create a mock response
        response = MagicMock()
        response.data = sample_csv_data_diff_delimiter.encode('windows-1252')
        response.status = 200
        return response

    # Setup the mock
    mock_pool_manager.return_value.request = mock_request_diff_delimiter

    # Call the function with a dummy URL and different delimiter
    url = "http://example.com/data.csv"
    result_df = get_data_from_github(url, delimiter=',')

    # Check if the DataFrame is not empty
    assert not result_df.empty


if __name__ == "__main__":
    pytest.main([__file__])