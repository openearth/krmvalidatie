from io import StringIO
import os
import pandas as pd
import urllib3
from typing import Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_data_from_github(
    url: str,
    encoding: str = 'windows-1252',
    delimiter: str = ';'
) -> Optional[pd.DataFrame]:
    """
    Fetch CSV data from a given URL and return it as a pandas DataFrame.

    Args:
        url (str): The URL to fetch the CSV data from.
        encoding (str): The encoding of the CSV data.
        delimiter (str): The delimiter used in the CSV data.

    Returns:
        Optional[pd.DataFrame]: A DataFrame containing the CSV data, or None if an error occurs.
    """
    try:
    
        # Send an HTTP GET request to the URL
        http = urllib3.PoolManager()
        response = http.request('GET', url)
        
        # Check if the request was successful
        if response.status != 200:
            logger.error(f"Failed to fetch data: HTTP {response.status}")
            return None
        
        # Convert the response content to a string and read it into a DataFrame
        csv_data = StringIO(response.data.decode(encoding))
        df = pd.read_csv(csv_data, delimiter=delimiter)

        df['new_index'] = range(1, len(df) + 1)
        
        return df
    
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None

def get_shape_data_from_github(url, local_filename, local_folder):
    
    # Create the full path for the local file
    local_file_path = os.path.join(local_folder, local_filename)

    # Create a urllib3.PoolManager instance
    http = urllib3.PoolManager()

    # Send a GET request to the URL
    response = http.request('GET', url)

    # Check if the request was successful
    if response.status == 200:
        # Write the content to the local file
        with open(local_file_path, 'wb') as f:
            f.write(response.data)
        print(f'File downloaded and saved as: {local_file_path}')
    else:
        print(f'Failed to download file: {response.status}')