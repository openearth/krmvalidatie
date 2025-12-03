<template>
  <v-app>
    <v-app-bar color="primary" dark>
      <v-app-bar-title>
        <v-icon class="mr-2">mdi-chart-line</v-icon>
        Data Bundle Visualization Dashboard
      </v-app-bar-title>
    </v-app-bar>

    <v-main>
      <v-container fluid>
        <DataDashboard />
      </v-container>
    </v-main>
  </v-app>
</template>

<script>
import DataDashboard from './components/DataDashboard.vue'

export default {
  name: 'App',
  components: {
    DataDashboard
  }
}
</script>

<style>
body {
  font-family: 'Roboto', sans-serif;
}
</style>