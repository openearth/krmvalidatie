<template>
  <v-dialog 
    :model-value="show" 
    max-width="800px"
    scrollable
    @update:model-value="$emit('update:show', $event)"
  >
    <v-card>
        <v-card-title :class="['text-h5', headerBackgroundColor]">
        <v-icon class="mr-2" :color="item?.combined_status === 'Voldoet met afwijkingen' ? 'yellow-darken-2' : 'red'">
            mdi-alert-circle
        </v-icon>
        Status Details: {{ item?.combined_status || 'N/A' }}
        <v-spacer />
        <v-btn
            icon="mdi-close"
            variant="text"
            @click="closeDialog"
        />
        </v-card-title>
      
      <v-divider />
      
      <v-card-text class="pa-6">
        <!-- Loading State -->
        <div v-if="loading" class="text-center py-8">
          <v-progress-circular indeterminate color="primary" size="50" />
          <div class="mt-4">Laden van details...</div>
        </div>
        
        <!-- Error State -->
        <div v-else-if="error" class="text-center py-8">
          <v-icon color="error" size="50">mdi-alert-circle</v-icon>
          <div class="mt-4 text-error">{{ error }}</div>
          <v-btn 
            color="primary" 
            variant="outlined" 
            class="mt-4"
            @click="$emit('refresh')"
          >
            Opnieuw proberen
          </v-btn>
        </div>
        
        <!-- Content -->
        <div v-else>
          <!-- Basic Bundle Information -->
          <BundleInformationCard :item="item" />

          <!-- Validation Issues -->
          <ValidationIssuesCard 
            :details="details"
            class="mb-4"
          />

          <!-- Next Steps -->
          <NextStepsCard :details="details" />
        </div>
      </v-card-text>

      <v-divider />

      <v-card-actions>
        <v-spacer />
        <v-btn
          color="grey-darken-1"
          variant="text"
          @click="closeDialog"
        >
          Sluiten
        </v-btn>
        <v-btn
          color="primary"
          variant="outlined"
          @click="$emit('refresh')"
        >
          <v-icon left>mdi-refresh</v-icon>
          Vernieuwen
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script>
import BundleInformationCard from './BundleInformationCard.vue'
import ValidationIssuesCard from './ValidationIssuesCard.vue'
import NextStepsCard from './NextStepsCard.vue'

export default {
  name: 'StatusDetailsDialog',
  components: {
    BundleInformationCard,
    ValidationIssuesCard,
    NextStepsCard
  },
  props: {
    show: {
      type: Boolean,
      required: true
    },
    loading: {
      type: Boolean,
      default: false
    },
    error: {
      type: String,
      default: null
    },
    item: {
      type: Object,
      default: null
    },
    details: {
      type: Object,
      default: null
    }
  },
  emits: ['update:show', 'close', 'refresh'],
  computed: {
    headerBackgroundColor() {
      if (!this.item?.combined_status) return 'bg-red-lighten-4';
      return this.item.combined_status === 'Voldoet met afwijkingen'
        ? 'bg-yellow-lighten-3'
        : 'bg-red-lighten-4';
    }
  },
  methods: {
    closeDialog() {
      this.$emit('update:show', false);
      this.$emit('close');
    }
  }
}
</script>
