import { s3Service } from './s3Service'
import { openEarthService } from './openEarthService'

// Fallback data for development/testing
const fallbackData = [
  {
    databundelcode: 'RWS_2023_05+vervuiling+vis+20240702_1580_rev',
    krmcriterium: 'ANSNL-D8C1',
    last_updated: '4-3-2025 08:43',
    validation: false,
    approval: false,
    status: 'Databundel validatie is: False en akkoord file is: False'
  },
]

export const dataParser = {
  async fetchData() {
    try {
      console.log('Attempting to fetch data from S3...')
      const csvData = await s3Service.fetchCsvData()
      return this.transformCsvData(csvData)
    } catch (error) {
      console.warn('Failed to fetch from S3, using fallback data:', error.message)
      return fallbackData
    }
  },

  async fetchDataViaUrl() {
    try {
      console.log('Attempting to fetch data via pre-signed URL...')
      const csvData = await s3Service.fetchCsvDataViaUrl()
      return this.transformCsvData(csvData)
    } catch (error) {
      console.warn('Failed to fetch via URL, using fallback data:', error.message)
      return fallbackData
    }
  },

  transformCsvData(csvData) {
    return csvData.map(row => {
      // Handle different possible column names from CSV
      const databundelcode = row.databundelcode || row.data_bundle_code || row['Data Bundle Code'] || ''
      const krmcriterium = row.krmcriterium || row.krm_criterium || row['KRM Criterium'] || ''
      const lastUpdated = row.last_updated || row.updated || row['Last Updated'] || ''
      
      // Handle validation status - look for various formats
      let validation = false
      let approval = false
      
      if (row.validation !== undefined) {
        validation = this.parseBoolean(row.validation)
      } else if (row.status) {
        const validationMatch = row.status.match(/validatie is: (True|False)/i)
        validation = validationMatch ? validationMatch[1].toLowerCase() === 'true' : false
      }
      
      if (row.approval !== undefined) {
        approval = this.parseBoolean(row.approval)
      } else if (row.status) {
        const approvalMatch = row.status.match(/akkoord file is: (True|False)/i)
        approval = approvalMatch ? approvalMatch[1].toLowerCase() === 'true' : false
      }

      return {
        databundelcode,
        krmcriterium,
        last_updated: lastUpdated,
        validation,
        approval,
        status: row.status || `Validatie: ${validation}, Akkoord: ${approval}`
      }
    }).filter(row => row.databundelcode) // Filter out rows without databundelcode
  },

  parseBoolean(value) {
    if (typeof value === 'boolean') return value
    if (typeof value === 'string') {
      return value.toLowerCase() === 'true' || value === '1' || value.toLowerCase() === 'yes'
    }
    return Boolean(value)
  }
}