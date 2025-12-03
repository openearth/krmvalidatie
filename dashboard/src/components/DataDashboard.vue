<template>
  <div>
    <!-- Statistics Cards -->
    <v-row class="stats-card">
      <v-col cols="12" sm="6" md="3">
        <v-card color="blue-lighten-1" dark>
          <v-card-text>
            <div class="text-h6">Totaal Bundels</div>
            <div class="text-h4">{{ totalBundles }}</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card color="orange-lighten-1" dark>
          <v-card-text>
            <div class="text-h6">Bundels geleverd</div>
            <div class="text-h4">{{ deliveredBundles }}</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card color="green-lighten-1" dark>
          <v-card-text>
            <div class="text-h6">Bundels gevalideerd</div>
            <div class="text-h4">{{ validBundles }}</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card color="purple-lighten-1" dark>
          <v-card-text>
            <div class="text-h6">Bundels gepubliceerd</div>
            <div class="text-h4">{{ publishedBundles }}</div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Filters -->
    <v-row>
      <v-col cols="12" md="2">
        <v-select
          v-model="selectedCriterium"
          :items="criteriumOptions"
          label="Filter KRM Criterium"
          clearable
          prepend-icon="mdi-filter"
        />
      </v-col>
      <v-col cols="12" md="2">
        <v-select
          v-model="selectedStatus"
          :items="statusOptions"
          label="Filter Status"
          clearable
          prepend-icon="mdi-check-circle"
        />
      </v-col>
      <v-col cols="12" md="2">
        <v-select
          v-model="selectedYear"
          :items="yearOptions"
          label="Filter Jaar (Leverdatum)"
          clearable
          prepend-icon="mdi-calendar"
        />
      </v-col>
      <v-col cols="12" md="2">
        <v-select
          v-model="selectedDeliveryStatus"
          :items="deliveryStatusOptions"
          label="Filter Levering Status"
          clearable
          prepend-icon="mdi-truck-delivery"
        />
      </v-col>
      <v-col cols="12" md="2">
        <v-select
          v-model="selectedPublicationStatus"
          :items="publicationStatusOptions"
          label="Filter Publicatie Status"
          clearable
          prepend-icon="mdi-truck-delivery"
        />
      </v-col>
    </v-row>

    <!-- Data Table -->
    <v-card class="data-table">
      <v-card-title>
        <v-icon class="mr-2">mdi-table</v-icon>
        Data Bundels Overzicht
        <v-spacer />
        <v-btn 
          color="primary" 
          variant="outlined" 
          @click="refreshData" 
          :loading="loading"
          class="mr-4"
        >
          <v-icon left>mdi-refresh</v-icon>
          Vernieuw
        </v-btn>
        <v-text-field
          v-model="search"
          append-icon="mdi-magnify"
          label="Search"
          single-line
          hide-details
          density="compact"
          style="max-width: 300px;"
        />
      </v-card-title>
      
      <v-alert 
        v-if="error" 
        type="error" 
        class="mx-4 mb-4"
        dismissible
        @click:close="error = null"
      >
        Error loading data: {{ error }}
      </v-alert>
      
      <v-data-table
        :headers="headers"
        :items="filteredData"
        :search="search"
        :loading="loading"
        loading-text="Loading data from database..."
        item-value="databundelcode"
        class="elevation-1"
        :items-per-page="10"
      >
        <template #item.databundelcode="{ item }">
          <v-tooltip :text="item.databundelcode">
            <template #activator="{ props }">
              <div v-bind="props" class="text-truncate" style="max-width: 200px;">
                {{ item.databundelcode }}
              </div>
            </template>
          </v-tooltip>
        </template>

        <template #item.krmcriterium="{ item }">
          <v-chip
            v-if="item.delivery_status === 'delivered'"
            :color="getCriteriumColor(item.krmcriterium)"
            size="small"
            class="status-chip clickable-chip"
            @click="handleCriteriumClick(item.krmcriterium, item.publication_status)"
            :loading="loadingCriterium === item.krmcriterium"
          >
            <v-icon left size="small">mdi-open-in-new</v-icon>
              {{ item.krmcriterium }}
          </v-chip>
        </template>

        <template #item.validation="{ item }">
          <v-chip
            :color="item.validation ? 'green' : 'red'"
            :prepend-icon="item.validation ? 'mdi-check' : 'mdi-close'"
            size="small"
            class="status-chip"
          >
            {{ item.validation ? 'Valid' : 'Invalid' }}
          </v-chip>
        </template>

        <template #item.approval="{ item }">
          <v-chip
            :color="item.approval ? 'green' : 'orange'"
            :prepend-icon="item.approval ? 'mdi-file-check' : 'mdi-file-alert'"
            size="small"
            class="status-chip"
          >
            {{ item.approval ? 'Approved' : 'Pending' }}
          </v-chip>
        </template>

        <template #item.delivery_status="{ item }">
          
          <v-chip
            :color="item.delivery_status === 'delivered' ? 'green' : 'red'"
            :prepend-icon="item.delivery_status === 'delivered' ? 'mdi-truck-check' : 'mdi-truck-alert'"
            size="small"
            class="status-chip"
          >
            {{ item.delivery_status === 'delivered' ? 'Geleverd' : 'Niet Geleverd' }}
          </v-chip>
        </template>

        <template #item.combined_status="{ item }">
          <v-chip
            :color="getStatusColor(item.combined_status)"
            :prepend-icon="getStatusIcon(item.combined_status)"
            size="small"
            :class="['status-chip', { 'clickable-chip': item.combined_status === 'Geleverd maar voldoet nog niet' || item.combined_status === 'Voldoet met afwijkingen' }]"
            @click="handleStatusClick(item)"
          >
            {{ item.combined_status }}
            <v-icon 
              v-if="item.combined_status === 'Geleverd maar voldoet nog niet' || item.combined_status === 'Voldoet met afwijkingen'"
              right 
              size="small"
            >
              mdi-information
            </v-icon>
          </v-chip>
        </template>

        <template #item.last_updated="{ item }">
          <div class="text-body-2">
            {{ formatDate(item.last_updated) }}
          </div>
        </template>

        <template #item.leverdatum="{ item }">
          <div class="text-body-2">
            {{ formatDate(item.leverdatum, true) }}
          </div>
        </template>

        <template #item.publication_status="{ item }">  
          <v-chip
            :color="item.publication_status === 'published' ? 'green' : 'red'"
            :prepend-icon="item.publication_status === 'published' ? 'mdi-truck-check' : 'mdi-truck-alert'"
            size="small"
            class="status-chip"
          >
            {{ item.publication_status === 'published' ? 'Gepubliceerd' : 'Niet Gepubliceerd' }}
          </v-chip>
        </template>
      </v-data-table>
    </v-card>

    <StatusDetailsDialog
      v-model:show="statusDialog.show"
      :loading="statusDialog.loading"
      :error="statusDialog.error"
      :item="statusDialog.selectedItem"
      :details="statusDialog.details"
      @close="handleDialogClose"
      @refresh="loadStatusDetails(statusDialog.selectedItem)"
    />

    <!-- Snackbar for notifications -->
    <v-snackbar
      v-model="snackbar.show"
      :color="snackbar.color"
      timeout="4000"
      location="bottom right"
    >
      {{ snackbar.message }}
      <template #actions>
        <v-btn
          variant="text"
          @click="snackbar.show = false"
        >
          Close
        </v-btn>
      </template>
    </v-snackbar>
  </div>
</template>

<script>
import { dataParser } from '../utils/dataParser'
import { openEarthService } from '../utils/openEarthService'
import { s3Service } from '../utils/s3Service'
import StatusDetailsDialog from './StatusDetailsDialog.vue'
import { determineSeverityFromIssue } from '../utils/severityHelpers'
import { formatDate, extractYearFromLeverdatum, calculateDeadline } from '../utils/dateFormatters'

export default {
  name: 'DataDashboard',
  components: {
    StatusDetailsDialog
  },
  data() {
    return {
      search: '',
      selectedCriterium: null,
      selectedStatus: null,
      selectedDeliveryStatus: null,
      selectedPublicationStatus: null,
      selectedYear: new Date().getFullYear(),
      data: [],
      validationData: [],
      publishedData: [],
      loading: true,
      loadingCriterium: null,
      error: null,
      snackbar: {
        show: false,
        message: '',
        color: 'success'
      },
      statusDialog: {
        show: false,
        loading: false,
        error: null,
        selectedItem: null,
        details: null
      },
      headers: [
        { title: 'Data Bundel Code', key: 'databundelcode', sortable: true },
        { title: 'KRM Criterium', key: 'krmcriterium', sortable: true },
        { title: 'Laatste Update', key: 'last_updated', sortable: true },
        { title: 'Leverdatum', key: 'leverdatum', sortable: true },
        { title: 'Status', key: 'combined_status', sortable: true },
        { title: 'Levering Status', key: 'delivery_status', sortable: true },
        { title: 'Publicatie Status', key: 'publication_status', sortable: true }
      ]
    }
  },
  async mounted() {
    await this.loadData()
  },
  computed: {
    totalBundles() {
      return this.filteredData.length
    },
    validBundles() {
      return this.filteredData.filter(item => item.validation).length
    },
    compliantBundles() {
      return this.filteredData.filter(item => 
        item.combined_status === 'Voldoet' || 
        item.combined_status === 'Voldoet met afwijkingen'
      ).length
    },
    deliveredBundles() {
      return this.filteredData.filter(item => item.delivery_status == 'delivered').length
    },
    publishedBundles() {
      return this.filteredData.filter(item => item.publication_status == 'published').length
    },
    uniqueCriteria() {
      return new Set(this.mergedData.map(item => item.krmcriterium)).size
    },
    criteriumOptions() {
      return [...new Set(this.mergedData.map(item => item.krmcriterium))].sort()
    },
    validationOptions() {
      return [
        { title: 'Valid', value: true },
        { title: 'Invalid', value: false }
      ]
    },
    approvalOptions() {
      return [
        { title: 'Approved', value: true },
        { title: 'Pending', value: false }
      ]
    },
    deliveryStatusOptions() {
      return [
        { title: 'Geleverd', value: 'delivered' },
        { title: 'Niet Geleverd', value: 'not_delivered' }
      ]
    },
    statusOptions() {
      return [
        { title: 'Voldoet', value: 'Voldoet' },
        { title: 'Voldoet met afwijkingen', value: 'Voldoet met afwijkingen' },
        { title: 'Geleverd maar voldoet nog niet', value: 'Geleverd maar voldoet nog niet' },
        { title: 'Gegevens nog niet beschikbaar', value: 'Gegevens nog niet beschikbaar' }
      ]
    },
    publicationStatusOptions() {
      return [
        { title: 'Gepubliceerd', value: 'published' },
        { title: 'Niet Gepubliceerd', value: 'not_published' }
      ]
    },
    yearOptions() {
      // Generate year options from the data, with 2025 as current year
      const years = new Set()
      this.mergedData.forEach(item => {
        if (item.leverdatum) {
          const year = extractYearFromLeverdatum(item.leverdatum)
          if (year) {
            years.add(year)
          }
        }
      })
      
      // Always include current year and a few surrounding years
      const currentYear = new Date().getFullYear()
      for (let i = currentYear - 2; i <= currentYear + 1; i++) {
        years.add(i)
      }
      
      return Array.from(years).sort((a, b) => b - a) // Sort descending (newest first)
    },
    mergedData() {
      return this.mergeValidationWithTableData()
    },
    filteredData() {
      let filtered = this.mergedData
      
      if (this.selectedCriterium) {
        filtered = filtered.filter(item => item.krmcriterium === this.selectedCriterium)
      }
      
      if (this.selectedStatus) {
        filtered = filtered.filter(item => item.combined_status === this.selectedStatus)
      }
      
      if (this.selectedDeliveryStatus !== null) {
        filtered = filtered.filter(item => item.delivery_status === this.selectedDeliveryStatus)
      }

      if (this.selectedPublicationStatus !== null) {
        filtered = filtered.filter(item => item.publication_status === this.selectedPublicationStatus)
      }

      // Filter by year if selected
      if (this.selectedYear !== null) {
        filtered = filtered.filter(item => {
          if (!item.leverdatum) return false
          const year = extractYearFromLeverdatum(item.leverdatum)
          return year === this.selectedYear
        })
      }
            
      return filtered
    }
  },
  methods: {
    formatDate,
    extractYearFromLeverdatum,
    async loadData() {
      try {
        this.loading = true
        this.error = null
        
        // Load both table data and validation data concurrently
        const [tableData, validationData, publishedData] = await Promise.all([
          dataParser.fetchData(),
          s3Service.fetchValidatielijstFromGithub(),
          s3Service.fetchCsvData('rapportages/publisheddata.csv')
        ])
        
        this.data = tableData
        this.validationData = validationData
        this.publishedData = publishedData

        console.log(publishedData)
        
        console.log(`Loaded ${tableData.length} table records and ${validationData.length} validation records`)
        
      } catch (error) {
        console.error('Error loading data:', error)
        this.error = error.message
      } finally {
        this.loading = false
      }
    },
    
    /**
     * Extracts the base databundelcode from a full databundel name
     * Example: "RWS_2023_05+vervuiling+vis+20240702_1580_rev" -> "RWS_2023_05"
     */
    extractBaseDatabundelcode(fullName) {
      if (!fullName) return ''
      
      // Split by '+' and take the first part, or split by space and take first part
      const parts = fullName.split('+')
      if (parts.length > 1) {
        return parts[0].trim()
      }
      
      // Fallback: take everything before the first space (for cases like "RWS_2021_10 zwerfvuil op strand")
      return fullName.split(' ')[0].trim()
    },
    
    /**
     * Groups databundels by their base code and returns only the latest version
     */
    getLatestDatabundels(tableData) {
      const grouped = {}
      
      tableData.forEach(item => {
        const baseCode = this.extractBaseDatabundelcode(item.databundelcode)
        
        if (!grouped[baseCode]) {
          grouped[baseCode] = []
        }
        
        grouped[baseCode].push(item)
      })
      
      // For each group, find the latest version based on last_updated
      const latestItems = []
      Object.keys(grouped).forEach(baseCode => {
        const items = grouped[baseCode]
        const latest = items.reduce((prev, current) => {
          const prevDate = new Date(prev.last_updated || '1970-01-01')
          const currentDate = new Date(current.last_updated || '1970-01-01')
          return currentDate > prevDate ? current : prev
        })
        
        // Add the base code for easier matching
        latest.base_databundelcode = baseCode
        latestItems.push(latest)
      })
      
      return latestItems
    },

    getPublishedStatus(databundelcode) {
      // Normalize the databundelcode: replace spaces with '+'
      const normalizedCode = databundelcode.replace(/\s+/g, '+');
      console.log(this.publishedData);
      console.log(normalizedCode);
      
      // Check if any item in the array has a databundelcode that starts with the normalized code
      const matchingItem = this.publishedData.find(
        item => item.databundelcode && item.databundelcode.replace(/\s+/g, '+').startsWith(normalizedCode)
      );
      
      if (matchingItem) {
        return 'published';
      } else {
        return 'not_published';
      }
    },
    
    /**
     * Merges validation data with table data, ensuring all validation entries are represented
     */
    mergeValidationWithTableData() {
      const latestTableData = this.getLatestDatabundels(this.data)
      const mergedData = []
      
      // Create a map of table data by base databundelcode for quick lookup
      const tableDataMap = {}
      latestTableData.forEach(item => {
        const baseCode = item.base_databundelcode || this.extractBaseDatabundelcode(item.databundelcode)
        tableDataMap[baseCode] = item
      })
      
      // Process each validation entry
      this.validationData.forEach(validationItem => {
        const baseCode = this.extractBaseDatabundelcode(validationItem.databundelcode)
        const tableItem = tableDataMap[baseCode]
        
        if (tableItem) {
          // Merge existing table data with validation data
          const combinedStatus = this.getCombinedStatus(tableItem.validation, tableItem.approval, 'delivered')
          
          mergedData.push({
            ...tableItem,
            leverdatum: validationItem.leverdatum,
            krmcriterium: validationItem.criteria || tableItem.krmcriterium,
            delivery_status: 'delivered',
            publication_status: this.getPublishedStatus(validationItem.databundelcode),
            combined_status: combinedStatus,
            validation_databundelcode: validationItem.databundelcode
          })
          
          // Remove from map so we don't process it again
          delete tableDataMap[baseCode]
        } else {
          // Create new entry for undelivered databundel
          const combinedStatus = this.getCombinedStatus(false, false, 'not_delivered')
          
          mergedData.push({
            databundelcode: validationItem.databundelcode,
            krmcriterium: validationItem.criteria,
            leverdatum: validationItem.leverdatum,
            last_updated: null,
            validation: false,
            approval: false,
            delivery_status: 'not_delivered',
            publication_status: 'not_published',
            combined_status: combinedStatus,
            validation_databundelcode: validationItem.databundelcode
          })
        }
      })
      
      // Add any remaining table items that weren't in validation data
      Object.values(tableDataMap).forEach(tableItem => {
        const combinedStatus = this.getCombinedStatus(tableItem.validation, tableItem.approval, 'delivered')
        
        mergedData.push({
          ...tableItem,
          delivery_status: 'delivered', // They exist in table so they're delivered
          combined_status: combinedStatus,
          publication_status: this.getPublishedStatus(tableItem.databundelcode),
          leverdatum: null // No validation data available
        })
      })
      
      return mergedData
    },
    
    async refreshData() {
      await this.loadData()
    },

    /**
     * Determines the combined status based on validation and approval
     * @param {boolean} validation - Whether the data validates
     * @param {boolean} approval - Whether approved (can override validation)
     * @param {string} deliveryStatus - Whether data is delivered or not
     * @returns {string} Combined status
     */
    getCombinedStatus(validation, approval, deliveryStatus) {
      // If not delivered, data is not available
      if (deliveryStatus === 'not_delivered') {
        return 'Gegevens nog niet beschikbaar'
      }
      
      // If approved, it either fully complies or complies with deviations
      if (approval) {
        return validation ? 'Voldoet' : 'Voldoet met afwijkingen'
      }
      
      // If validation passes but not approved
      if (validation) {
        return 'Voldoet'
      }
      
      // If both validation and approval are false/negative
      return 'Geleverd maar voldoet nog niet'
    },

    /**
     * Gets the appropriate color for a status
     */
    getStatusColor(status) {
      switch (status) {
        case 'Voldoet':
          return 'green'
        case 'Voldoet met afwijkingen':
          return 'orange'
        case 'Geleverd maar voldoet nog niet':
          return 'red'
        case 'Gegevens nog niet beschikbaar':
          return 'grey'
        default:
          return 'grey'
      }
    },

    /**
     * Gets the appropriate icon for a status
     */
    getStatusIcon(status) {
      switch (status) {
        case 'Voldoet':
          return 'mdi-check-circle'
        case 'Voldoet met afwijkingen':
          return 'mdi-check-circle-outline'
        case 'Geleverd maar voldoet nog niet':
          return 'mdi-close-circle'
        case 'Gegevens nog niet beschikbaar':
          return 'mdi-help-circle'
        default:
          return 'mdi-help-circle'
      }
    },

    
    /**
     * Handles status chip clicks - only opens dialog for "Geleverd maar voldoet nog niet"
     */
    async handleStatusClick(item) {
      if (item.combined_status === 'Geleverd maar voldoet nog niet'  || item.combined_status === 'Voldoet met afwijkingen') {
        this.statusDialog.selectedItem = item
        this.statusDialog.show = true
        await this.loadStatusDetails(item)
      }
    },

    /**
     * Transforms databundelcode to S3 filename format
     * Example: "RWS_2022_10+zwerfvuil+op+strand+20231109" -> "validatielijst_per_locatie_met_aantal_RWS_2022_10 zwerfvuil op strand 20231109.csv"
     */
    transformToS3Filename(databundelcode) {
      if (!databundelcode) return null
      
      // Replace + signs with spaces
      const transformed = databundelcode.replace(/\+/g, ' ')
      
      // Add the prefix and suffix
      const filename = `validatielijst_per_locatie_met_aantal_${transformed}.csv`
      
      console.log('Transformed databundelcode to filename:', databundelcode, '->', filename)
      return filename
    },

    /**
     * Loads detailed status information from S3
     */
    async loadStatusDetails(item) {
      try {
        this.statusDialog.loading = true
        this.statusDialog.error = null
        
        const filename = this.transformToS3Filename(item.databundelcode)
        
        if (!filename) {
          throw new Error('Ongeldig databundelcode format')
        }

        const statusDetails = await s3Service.fetchStatusDetails(filename)
        
        if (!statusDetails) {
          throw new Error(`Geen status details gevonden voor ${filename}`)
        }

        const parsedDetails = this.parseStatusDetailsFromCSV(statusDetails)
        this.statusDialog.details = parsedDetails
        
      } catch (error) {
        console.error('Error loading status details:', error)
        this.statusDialog.error = `Fout bij het laden van details: ${error.message}`
      } finally {
        this.statusDialog.loading = false
      }
    },

    /**
     * Parses CSV data into structured validation issues and next steps
     * Based on the actual CSV structure with columns: databundelcode, record_id, locatiecode_aantal, aantaldat, limiet, aantalval, uitvalreden, recordnrs, validatieregel
     */
    parseStatusDetailsFromCSV(csvData) {
      try {
        const validationIssues = []
        const nextSteps = []
        const locationSummary = {}
        
        if (csvData && csvData.length > 0) {
          csvData.forEach((row, index) => {
            // Skip empty rows or header-like rows
            if (!row.databundelcode || !row.uitvalreden) return
            
            // Parse the validatieregel JSON if it exists
            let validationRuleInfo = null
            try {
              if (row.validatieregel && typeof row.validatieregel === 'string') {
                // Clean up the JSON string and parse it
                const cleanJson = row.validatieregel.replace(/'/g, '"').replace(/nan/g, 'null')
                validationRuleInfo = JSON.parse(cleanJson)
              }
            } catch (e) {
              console.warn('Could not parse validatieregel JSON:', e)
            }
            
            // Determine severity based on the validation issue
            const severity = determineSeverityFromIssue(row.uitvalreden)
            
            // Create validation issue
            const issue = {
              severity: severity,
              category: 'Data Validatie',
              description: this.formatIssueDescription(row),
              field: 'aantal monsters',
              expectedValue: `${row.aantaldat} (${row.limiet})`,
              actualValue: row.aantalval?.toString() || 'Onbekend',
              recommendation: this.generateRecommendation(row),
              location: row.locatiecode_aantal || 'Onbekend',
              recordId: row.record_id,
              validationRule: validationRuleInfo
            }
            
            validationIssues.push(issue)
            
            // Track location summary for statistics
            if (!locationSummary[row.locatiecode_aantal]) {
              locationSummary[row.locatiecode_aantal] = {
                count: 0,
                issues: []
              }
            }
            locationSummary[row.locatiecode_aantal].count++
            locationSummary[row.locatiecode_aantal].issues.push(row.uitvalreden)
          })
        }

        // Generate next steps based on validation issues
        const totalIssues = validationIssues.length
        const affectedLocations = Object.keys(locationSummary).length
        const uniqueIssueTypes = [...new Set(validationIssues.map(issue => issue.description))].length
        
        if (totalIssues > 0) {
          // High priority: Fix data collection issues
          if (validationIssues.some(issue => issue.severity === 'High')) {
            nextSteps.push({
              description: `Corrigeer kritieke data problemen (${validationIssues.filter(i => i.severity === 'High').length} gevallen)`,
              completed: false,
              deadline: calculateDeadline(7)
            })
          }
          
          // Medium priority: Address sample count discrepancies
          nextSteps.push({
            description: `Controleer monster aantal inconsistenties op ${affectedLocations} locatie(s)`,
            completed: false,
            deadline: calculateDeadline(14)
          })
          
          // Review data collection methodology
          if (uniqueIssueTypes > 1) {
            nextSteps.push({
              description: 'Evalueer data verzamel methodologie',
              completed: false,
              deadline: calculateDeadline(21)
            })
          }
          
          // Resubmit corrected data
          nextSteps.push({
            description: 'Herzend gecorrigeerde dataset na fixes',
            completed: false,
            deadline: calculateDeadline(28)
          })
          
          // Final validation
          nextSteps.push({
            description: 'Nieuwe validatie aanvragen',
            completed: false,
            deadline: calculateDeadline(35)
          })
        }

        return {
          validationIssues,
          nextSteps,
          totalIssues,
          affectedLocations,
          locationSummary,
          csvRowCount: csvData.length
        }
        
      } catch (error) {
        console.error('Error parsing CSV status details:', error)
        throw new Error(`Fout bij het verwerken van CSV data: ${error.message}`)
      }
    },

    /**
     * Formats a human-readable issue description
     */
    formatIssueDescription(row) {
      const location = row.locatiecode_aantal || 'Onbekende locatie'
      const expected = row.aantaldat || 'onbekend'
      const actual = row.aantalval || 'onbekend'
      const reason = row.uitvalreden || 'Onbekende reden'
      
      return `${reason} op locatie ${location}. Verwacht: ${expected}, Gevonden: ${actual}`
    },

    /**
     * Generates recommendations based on the validation issue
     */
    generateRecommendation(row) {
      const reason = row.uitvalreden?.toLowerCase() || ''
      
      if (reason.includes('aantal monsters ongelijk')) {
        return `Controleer waarom er ${row.aantalval || 'een afwijkend aantal'} monsters zijn gevonden in plaats van de verwachte ${row.aantaldat || 'hoeveelheid'}. Mogelijk is er een probleem met de bemonsteringsmethode of zijn er monsters verloren gegaan.`
      }
      
      if (reason.includes('ontbreekt')) {
        return 'Voeg de ontbrekende data toe of verzamel aanvullende monsters op deze locatie.'
      }
      
      if (reason.includes('format') || reason.includes('formaat')) {
        return 'Corrigeer het dataformaat zodat het voldoet aan de verwachte specificaties.'
      }
      
      // Generic recommendation
      return `Onderzoek en corrigeer de oorzaak van: ${row.uitvalreden}. Neem indien nodig contact op met het data team voor ondersteuning.`
    },

    async handleCriteriumClick(criterium, publication_status) {
      try {
        console.log(publication_status)
        this.loadingCriterium = criterium
        console.log('Clicked on criterium:', criterium)
        
        const viewerInfo = await openEarthService.getCachedViewerUrl(criterium, publication_status)
        
        if (viewerInfo && viewerInfo.url) {
          console.log('Opening viewer URL:', viewerInfo.url)
          // Open in new tab
          window.open(viewerInfo.url, '_blank', 'noopener,noreferrer')
          
          // Show success message
          this.showSnackbar(`Opened viewer for ${viewerInfo.layerName}`, 'success')
        } else {
          console.warn('No viewer URL found for criterium:', criterium)
          this.showSnackbar(`No viewer data found for criterium: ${criterium}`, 'warning')
        }
      } catch (error) {
        console.error('Error opening criterium viewer:', error)
        this.showSnackbar(`Error loading viewer for ${criterium}: ${error.message}`, 'error')
      } finally {
        this.loadingCriterium = null
      }
    },
    showSnackbar(message, color = 'success') {
      this.snackbar.message = message
      this.snackbar.color = color
      this.snackbar.show = true
    },
    getCriteriumColor(criterium) {
      const colors = ['blue', 'green', 'orange', 'purple', 'teal', 'indigo', 'pink', 'cyan']
      const hash = criterium.split('').reduce((a, b) => {
        a = ((a << 5) - a) + b.charCodeAt(0)
        return a & a
      }, 0)
      return colors[Math.abs(hash) % colors.length]
    },
  }
}
</script>

<style scoped>
.status-chip {
  font-size: 0.75rem;
}

.clickable-chip {
  cursor: pointer;
  transition: all 0.2s ease;
}

.clickable-chip:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
}

.data-table {
  margin-top: 20px;
}

.stats-card {
  margin-bottom: 20px;
}
</style>