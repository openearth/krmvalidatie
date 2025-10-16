<template>
  <div class="mt-2">
    <div class="text-subtitle-2 mb-2">Probleem beschrijving:</div>
    <div class="text-body-2 mb-3 bg-grey-lighten-5 pa-3 rounded">
      {{ issue.description }}
    </div>
    
    <v-row class="mb-3">
      <v-col cols="6">
        <div class="text-subtitle-2 mb-1">Verwacht:</div>
        <div class="text-body-2 bg-green-lighten-5 pa-2 rounded">
          {{ issue.expectedValue }}
        </div>
      </v-col>
      <v-col cols="6">
        <div class="text-subtitle-2 mb-1">Gevonden:</div>
        <div class="text-body-2 bg-red-lighten-5 pa-2 rounded">
          {{ issue.actualValue }}
        </div>
      </v-col>
    </v-row>
    
    <div v-if="issue.recommendation" class="mb-3">
      <div class="text-subtitle-2 mb-1">Aanbeveling:</div>
      <div class="text-body-2 bg-blue-lighten-5 pa-3 rounded">
        {{ issue.recommendation }}
      </div>
    </div>

    <!-- Show validation rule info if available -->
    <div v-if="issue.validationRule" class="mt-3">
      <v-expansion-panels variant="accordion" class="mb-0">
        <v-expansion-panel>
          <v-expansion-panel-title class="text-caption">
            <v-icon class="mr-2" size="small">mdi-code-braces</v-icon>
            Validatie regel details
          </v-expansion-panel-title>
          <v-expansion-panel-text>
            <div class="text-caption">
              <div v-if="issue.validationRule.criteria" class="mb-1">
                <strong>Criterium:</strong> {{ issue.validationRule.criteria }}
              </div>
              <div v-if="issue.validationRule.grootheid_code" class="mb-1">
                <strong>Grootheid:</strong> {{ issue.validationRule.grootheid_code }}
              </div>
              <div v-if="issue.validationRule.groep" class="mb-1">
                <strong>Groep:</strong> {{ issue.validationRule.groep }}
              </div>
              <div v-if="issue.validationRule.leverdatum" class="mb-1">
                <strong>Leverdatum:</strong> {{ issue.validationRule.leverdatum }}
              </div>
            </div>
          </v-expansion-panel-text>
        </v-expansion-panel>
      </v-expansion-panels>
    </div>
  </div>
</template>

<script>
export default {
  name: 'ValidationIssueDetails',
  props: {
    issue: {
      type: Object,
      required: true
    }
  }
}
</script>