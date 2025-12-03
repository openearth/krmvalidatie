<template>
  <v-card variant="outlined">
    <v-card-title class="text-h6 bg-orange-lighten-4">
      <v-icon class="mr-2" color="orange">mdi-alert-outline</v-icon>
      Validatie Problemen
      <v-spacer />
      <v-chip 
        v-if="details && details.totalIssues"
        color="orange" 
        size="small"
      >
        {{ details.totalIssues }} problemen gevonden
      </v-chip>
    </v-card-title>
    <v-card-text>
      <!-- Summary Statistics -->
      <div v-if="details && details.locationSummary" class="mb-4">
        <v-row>
          <v-col cols="12" md="4">
            <v-card variant="tonal" color="info" class="text-center pa-3">
              <div class="text-h4">{{ details.totalIssues }}</div>
              <div class="text-caption">Totaal problemen</div>
            </v-card>
          </v-col>
          <v-col cols="12" md="4">
            <v-card variant="tonal" color="warning" class="text-center pa-3">
              <div class="text-h4">{{ details.affectedLocations }}</div>
              <div class="text-caption">Betrokken locaties</div>
            </v-card>
          </v-col>
          <v-col cols="12" md="4">
            <v-card variant="tonal" color="error" class="text-center pa-3">
              <div class="text-h4">{{ highSeverityCount }}</div>
              <div class="text-caption">Hoge prioriteit</div>
            </v-card>
          </v-col>
        </v-row>
      </div>

      <!-- Issues List -->
      <div v-if="hasValidationIssues">
        <v-expansion-panels variant="accordion">
          <v-expansion-panel
            v-for="(issue, index) in details.validationIssues"
            :key="index"
          >
            <v-expansion-panel-title>
              <div class="d-flex align-center w-100">
                <v-icon 
                  :color="getIssueSeverityColor(issue.severity)" 
                  class="mr-3"
                >
                  {{ getIssueSeverityIcon(issue.severity) }}
                </v-icon>
                <div class="flex-grow-1">
                  <div class="font-weight-medium">{{ issue.location }}</div>
                  <div class="text-caption text-grey-darken-1">{{ issue.recordId }}</div>
                </div>
                <v-chip
                  :color="getIssueSeverityColor(issue.severity)"
                  size="small"
                  class="mr-2"
                >
                  {{ issue.severity }}
                </v-chip>
              </div>
            </v-expansion-panel-title>
            <v-expansion-panel-text>
              <ValidationIssueDetails :issue="issue" />
            </v-expansion-panel-text>
          </v-expansion-panel>
        </v-expansion-panels>
      </div>
      
      <!-- Empty State -->
      <div v-else class="text-center py-4 text-grey">
        <v-icon size="40" color="grey">mdi-information-outline</v-icon>
        <div class="mt-2">Geen validatie problemen gevonden in de details.</div>
      </div>
    </v-card-text>
  </v-card>
</template>

<script>
import ValidationIssueDetails from './ValidationIssueDetails.vue'
import { getIssueSeverityColor, getIssueSeverityIcon } from '../utils/severityHelpers'

export default {
  name: 'ValidationIssuesCard',
  components: {
    ValidationIssueDetails
  },
  props: {
    details: {
      type: Object,
      default: null
    }
  },
  computed: {
    hasValidationIssues() {
      return this.details && 
             this.details.validationIssues && 
             this.details.validationIssues.length > 0
    },
    highSeverityCount() {
      if (!this.hasValidationIssues) return 0
      return this.details.validationIssues.filter(i => i.severity === 'High').length
    }
  },
  methods: {
    getIssueSeverityColor,
    getIssueSeverityIcon
  }
}
</script>