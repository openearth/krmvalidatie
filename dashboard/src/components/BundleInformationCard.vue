<template>
  <v-card class="mb-4" variant="outlined">
    <v-card-title class="text-h6 bg-grey-lighten-4">
      <v-icon class="mr-2">mdi-package-variant</v-icon>
      Bundel Informatie
    </v-card-title>
    <v-card-text>
      <v-row>
        <v-col cols="12" md="6">
          <div class="text-subtitle-2 text-grey-darken-1">Data Bundel Code</div>
          <div class="text-body-1 mb-3">{{ item?.databundelcode || 'N/A' }}</div>
          
          <div class="text-subtitle-2 text-grey-darken-1">KRM Criterium</div>
          <div class="text-body-1 mb-3">{{ item?.krmcriterium || 'N/A' }}</div>
        </v-col>
        <v-col cols="12" md="6">
          <div class="text-subtitle-2 text-grey-darken-1">Leverdatum</div>
          <div class="text-body-1 mb-3">{{ formatDate(item?.leverdatum, true) }}</div>
          
          <div class="text-subtitle-2 text-grey-darken-1">Laatste Update</div>
          <div class="text-body-1 mb-3">{{ formatDate(item?.last_updated) }}</div>
        </v-col>
      </v-row>
    </v-card-text>
  </v-card>
</template>

<script>
import { formatDate } from '../utils/dateFormatters'

export default {
  name: 'BundleInformationCard',
  props: {
    item: {
      type: Object,
      default: null
    }
  },
  methods: {
    formatDate
  }
}
</script>