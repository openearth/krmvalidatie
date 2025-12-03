/**
 * Utility functions for handling validation issue severity levels
 */

/**
 * Gets the appropriate color for an issue severity level
 * @param {string} severity - The severity level (High, Medium, Low, etc.)
 * @returns {string} Vuetify color string
 */
export function getIssueSeverityColor(severity) {
  if (!severity) return 'grey'
  
  switch (severity.toLowerCase()) {
    case 'high':
    case 'hoog':
      return 'red'
    case 'medium':
    case 'middel':
      return 'orange'
    case 'low':
    case 'laag':
      return 'yellow'
    default:
      return 'grey'
  }
}

/**
 * Gets the appropriate Material Design icon for an issue severity level
 * @param {string} severity - The severity level (High, Medium, Low, etc.)
 * @returns {string} MDI icon name
 */
export function getIssueSeverityIcon(severity) {
  if (!severity) return 'mdi-information-outline'
  
  switch (severity.toLowerCase()) {
    case 'high':
    case 'hoog':
      return 'mdi-alert-circle'
    case 'medium':
    case 'middel':
      return 'mdi-alert'
    case 'low':
    case 'laag':
      return 'mdi-alert-outline'
    default:
      return 'mdi-information-outline'
  }
}

/**
 * Determines severity level based on the issue description
 * @param {string} uitvalreden - The validation failure reason in Dutch
 * @returns {string} Severity level ('High', 'Medium', or 'Low')
 */
export function determineSeverityFromIssue(uitvalreden) {
  if (!uitvalreden) return 'Low'
  
  const issue = uitvalreden.toLowerCase()
  
  // High severity issues
  if (issue.includes('kritiek') || issue.includes('ontbreekt') || issue.includes('fout')) {
    return 'High'
  }
  
  // Medium severity issues
  if (issue.includes('ongelijk') || issue.includes('verwachting') || issue.includes('afwijking')) {
    return 'Medium'
  }
  
  // Default to Low
  return 'Low'
}