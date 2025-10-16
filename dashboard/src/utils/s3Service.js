import Papa from 'papaparse';

export const s3Service = {
  async fetchCsvData(objectKey = 'rapportages/akkoorddata.csv') {
    try {
      console.log('Requesting pre-signed URL from backend...');

      // Your API Gateway endpoint
      const apiUrl = `https://fe9dopz8y1.execute-api.eu-west-1.amazonaws.com/default/krm-dashboard-dev?objectKey=${encodeURIComponent(objectKey)}`;

      const response = await fetch(apiUrl, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
        },
        mode: 'cors',
      });

      console.log('Response status:', response.status);
      console.log('Response headers:', [...response.headers.entries()]);

      if (!response.ok) {
        const errorText = await response.text();
        console.error('API Error Response:', errorText);
        throw new Error(`API request failed: ${response.status} - ${errorText}`);
      }

      const data = await response.json();
      console.log('API Response:', data);

      if (!data.downloadUrl) {
        throw new Error('No download URL received from API');
      }

      console.log('Got pre-signed URL, downloading file...');

      const fileResponse = await fetch(data.downloadUrl, {
        method: 'GET',
        mode: 'cors',
      });

      if (!fileResponse.ok) {
        throw new Error(`Failed to download file: ${fileResponse.status}`);
      }

      const csvContent = await fileResponse.text();
      console.log('CSV data fetched successfully, length:', csvContent.length);

      return new Promise((resolve, reject) => {
        Papa.parse(csvContent, {
          header: true,
          skipEmptyLines: true,
          dynamicTyping: true,
          transformHeader: (header) => {
            return header.trim().toLowerCase().replace(/\s+/g, '_');
          },
          complete: (results) => {
            if (results.errors.length > 0) {
              console.warn('CSV parsing warnings:', results.errors);
            }
            console.log(`Parsed ${results.data.length} rows`);
            resolve(results.data);
          },
          error: (error) => {
            reject(error);
          }
        });
      });

    } catch (error) {
      console.error('Error fetching CSV data:', error);

      if (error.name === 'TypeError' && error.message.includes('NetworkError')) {
        console.error('This is likely a CORS issue. Check:');
        console.error('1. Lambda function returns proper CORS headers');
        console.error('2. API Gateway has CORS enabled');
        console.error('3. API Gateway is deployed after CORS changes');
      }

      throw error;
    }
  },

/**
   * Downloads the validatielijst.csv file from GitHub and extracts only the required fields
   * Returns unique records based on databundelcode
   * @returns {Promise<Array>} Array of unique objects with databundelcode, criteria, and leverdatum
   */
  async fetchValidatielijstFromGithub() {
    try {
      console.log('Downloading validatielijst.csv from GitHub...');
      
      // Direct raw file URL from GitHub
      const githubRawUrl = 'https://raw.githubusercontent.com/openearth/krmvalidatie/main/data/validatielijst.csv';
      
      const response = await fetch(githubRawUrl, {
        method: 'GET',
        headers: {
          'Accept': 'text/csv',
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to download file from GitHub: ${response.status} - ${response.statusText}`);
      }

      const csvContent = await response.text();
      console.log('GitHub CSV data fetched successfully, length:', csvContent.length);

      // Parse CSV with Papa Parse
      return new Promise((resolve, reject) => {
        Papa.parse(csvContent, {
          header: true,
          skipEmptyLines: true,
          delimiter: ';', // The file uses semicolon as delimiter
          transformHeader: (header) => {
            return header.trim();
          },
          complete: (results) => {
            if (results.errors.length > 0) {
              console.warn('CSV parsing warnings:', results.errors);
            }
            
            // Filter to only include the columns you need
            const filteredData = results.data.map(row => ({
              databundelcode: row.databundelcode || '',
              criteria: row.criteria || '',
              leverdatum: row.leverdatum || ''
            }));

            // Remove duplicates based on databundelcode using Map
            const uniqueDataMap = new Map();
            filteredData.forEach(item => {
              if (item.databundelcode && item.criteria && item.leverdatum) {
                // If we already have this databundelcode, keep the one with the latest leverdatum
                if (uniqueDataMap.has(item.databundelcode)) {
                  const existing = uniqueDataMap.get(item.databundelcode);
                  
                  // Compare dates (assuming format like "15-9-2022")
                  const parseDate = (dateStr) => {
                    const [day, month, year] = dateStr.split('-');
                    return new Date(year, month - 1, day);
                  };
                  
                  try {
                    const existingDate = parseDate(existing.leverdatum);
                    const currentDate = parseDate(item.leverdatum);
                    
                    // Keep the record with the latest leverdatum
                    if (currentDate > existingDate) {
                      uniqueDataMap.set(item.databundelcode, item);
                    }
                  } catch (error) {
                    console.warn('Date parsing error, keeping first occurrence:', error);
                    // If date parsing fails, keep the first occurrence
                  }
                } else {
                  uniqueDataMap.set(item.databundelcode, item);
                }
              }
            });

            const uniqueData = Array.from(uniqueDataMap.values());

            console.log(`Parsed ${filteredData.length} rows with filtered columns`);
            console.log(`Reduced to ${uniqueData.length} unique databundel records`);
            console.log('Sample row:', uniqueData[0]);
            
            resolve(uniqueData);
          },
          error: (error) => {
            console.error('CSV parsing error:', error);
            reject(error);
          }
        });
      });
    } catch (error) {
      console.error('Error fetching validatielijst data from GitHub:', error);
      throw error;
    }
  },

  /**
   * Fetches status details CSV file from S3 based on filename
   * @param {string} filename - The transformed filename for the status details CSV
   * @returns {Promise<Array>} Parsed CSV data as array of objects
   */
  async fetchStatusDetails(filename) {
    try {
      console.log('Fetching status details from S3:', filename)
      
      // Construct the object key for the fetchCsvData function
      const objectKey = `rapportages/${filename}`
      
      console.log('Using object key:', objectKey)
      
      // Use the existing fetchCsvData function which handles:
      // - API Gateway request for pre-signed URL
      // - File download
      // - CSV parsing with Papa Parse
      const parsedData = await this.fetchCsvData(objectKey)
      
      console.log(`Successfully parsed ${parsedData.length} rows from status details CSV`)
      
      return parsedData
      
    } catch (error) {
      console.error('Error fetching status details from S3:', error)
      
      // Provide more specific error messages
      if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
        throw new Error('Kan geen verbinding maken met S3. Controleer de netwerkverbinding.')
      } else if (error.message.includes('404')) {
        throw new Error(`Status details bestand niet gevonden voor: ${filename}`)
      } else if (error.message.includes('403')) {
        throw new Error('Geen toegang tot het status details bestand. Controleer de permissies.')
      }
      
      throw error
    }
  },

  // Test function to check if API is working
  async testApiConnection() {
    try {
      const apiUrl = 'https://fe9dopz8y1.execute-api.eu-west-1.amazonaws.com/default/krm-dashboard-dev';
      
      console.log('Testing API connection...');
      const response = await fetch(apiUrl, {
        method: 'GET',
        mode: 'cors',
      });
      
      console.log('Test response status:', response.status);
      console.log('Test response headers:', [...response.headers.entries()]);
      
      const text = await response.text();
      console.log('Test response body:', text);
      
      return { status: response.status, body: text };
    } catch (error) {
      console.error('API test failed:', error);
      throw error;
    }
  }
};