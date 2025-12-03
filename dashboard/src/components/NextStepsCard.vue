<template>
  <v-card variant="outlined">
    <v-card-title class="text-h6 bg-blue-lighten-4">
      <v-icon class="mr-2" color="blue">mdi-list-status</v-icon>
      Volgende Stappen
    </v-card-title>
    <v-card-text>
      <div v-if="hasNextSteps">
        <v-list density="compact">
          <v-list-item
            v-for="(step, index) in details.nextSteps"
            :key="index"
            :prepend-icon="step.completed ? 'mdi-check-circle' : 'mdi-circle-outline'"
            :class="step.completed ? 'text-green' : ''"
          >
            <v-list-item-title>{{ step.description }}</v-list-item-title>
            <v-list-item-subtitle v-if="step.deadline">
              Deadline: {{ formatDate(step.deadline, true) }}
            </v-list-item-subtitle>
          </v-list-item>
        </v-list>
      </div>
      <div v-else>
        <v-list density="compact">
          <v-list-item prepend-icon="mdi-circle-outline">
            <v-list-item-title>Validatie problemen oplossen</v-list-item-title>
          </v-list-item>
          <v-list-item prepend-icon="mdi-circle-outline">
            <v-list-item-title>Herzenden van gecorrigeerde data</v-list-item-title>
          </v-list-item>
          <v-list-item prepend-icon="mdi-circle-outline">
            <v-list-item-title>Nieuwe validatie aanvragen</v-list-item-title>
          </v-list-item>
        </v-list>
      </div>
    </v-card-text>
  </v-card>
</template>

<script>
import { formatDate } from '../utils/dateFormatters'

export default {
  name: 'NextStepsCard',
  props: {
    details: {
      type: Object,
      default: null
    }
  },
  computed: {
    hasNextSteps() {
      return this.details && this.details.nextSteps && this.details.nextSteps.length > 0
    }
  },
  methods: {
    formatDate
  }
}
</script>