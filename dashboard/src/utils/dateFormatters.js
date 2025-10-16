/**
 * Utility functions for formatting dates in various contexts
 */

/**
 * Formats a date string for display
 * @param {string} dateStr - The date string to format
 * @param {boolean} isLeverdatum - Whether this is a leverdatum (delivery date) format
 * @returns {string} Formatted date string
 */
export function formatDate(dateStr, isLeverdatum = false) {
  if (!dateStr) return isLeverdatum ? 'Geen leverdatum' : 'Geen datum'
  
  try {
    let date
    
    if (isLeverdatum && dateStr.includes('-') && dateStr.split('-').length === 3) {
      // Handle leverdatum format like "15-9-2022" (day-month-year)
      const [day, month, year] = dateStr.split('-')
      date = new Date(`${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`)
    } else {
      // Handle regular datetime format
      const [datePart, timePart] = dateStr.split(' ')
      if (datePart && timePart) {
        const [year, month, day] = datePart.split('-')
        date = new Date(`${year}-${month}-${day}T${timePart}`)
      } else {
        date = new Date(dateStr)
      }
    }
    
    return date.toLocaleDateString('en-GB', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      ...(isLeverdatum ? {} : { hour: '2-digit', minute: '2-digit' })
    })
  } catch (e) {
    console.warn('Date formatting error:', e, dateStr)
    return dateStr
  }
}

/**
 * Extracts the year from a leverdatum string
 * Handles various date formats: "15-9-2022", "2022-09-15", etc.
 * @param {string} leverdatum - The leverdatum string
 * @returns {number|null} The extracted year or null if unable to parse
 */
export function extractYearFromLeverdatum(leverdatum) {
  if (!leverdatum) return null
  
  try {
    // Handle different date formats
    if (leverdatum.includes('-')) {
      const parts = leverdatum.split('-')
      
      // Format: "15-9-2022" (day-month-year)
      if (parts.length === 3 && parts[2].length === 4) {
        return parseInt(parts[2])
      }
      
      // Format: "2022-09-15" (year-month-day)
      if (parts.length >= 3 && parts[0].length === 4) {
        return parseInt(parts[0])
      }
    }
    
    // Try to parse as date and extract year
    const date = new Date(leverdatum)
    if (!isNaN(date.getTime())) {
      return date.getFullYear()
    }
    
    // Try to extract 4-digit year from string
    const yearMatch = leverdatum.match(/\b(19|20)\d{2}\b/)
    if (yearMatch) {
      return parseInt(yearMatch[0])
    }
    
    return null
  } catch (error) {
    console.warn('Error extracting year from leverdatum:', leverdatum, error)
    return null
  }
}

/**
 * Calculates a deadline date based on days from current date
 * @param {number} daysFromNow - Number of days to add to current date
 * @returns {string} Date string in YYYY-MM-DD format
 */
export function calculateDeadline(daysFromNow) {
  const deadline = new Date()
  deadline.setDate(deadline.getDate() + daysFromNow)
  return deadline.toISOString().split('T')[0]
}