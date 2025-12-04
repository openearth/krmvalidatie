export const openEarthService = {
  baseUrl: 'https://kaartenbak.netlify.app/api/search',
  viewerUrl: 'https://viewer.openearth.nl/ihm-viewer/',
  // Clean the criterium by removing ANSL- or ANSNL- prefix
  cleanCriterium(criterium) {
    return criterium.replace(/^ANSNL?-/, '');
  },
  async searchLayers(criterium, publication_status) {
    try {
      const cleanedCriterium = this.cleanCriterium(criterium);
      let searchUrl;
      if (publication_status === 'not_published') {
        searchUrl = `${this.baseUrl}?viewer=IHM_KRM_test&query=${encodeURIComponent(cleanedCriterium)}`;
      } else {
        searchUrl = `${this.baseUrl}?viewer=IHM%20viewer&query=${encodeURIComponent(cleanedCriterium)}`;
      }
      console.log('Searching OpenEarth for:', cleanedCriterium, '(original:', criterium + ')', 'URL:', searchUrl);

      const response = await fetch(searchUrl);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      console.log('OpenEarth search results:', data);

      if (publication_status === 'not_published') {
        this.viewerUrl = 'https://viewer.openearth.nl/ihm-krm-test/';
        // For not_published, use the specific test layer
      } else {
        this.viewerUrl = 'https://viewer.openearth.nl/ihm-viewer/';
      }

      return data;

    } catch (error) {
      console.error('Error searching OpenEarth:', error);
      throw error;
    }
  },
  buildViewerUrl(layerIds, layerNames) {
    const encodedLayerNames = layerNames.map(name => encodeURIComponent(name)).join(',');
    return `${this.viewerUrl}?layers=${layerIds.join(',')}&layerNames=${encodedLayerNames}`;
  },
  async getViewerUrlForCriterium(criterium, publication_status) {
    try {
      // Split the criterium into individual criteria
      const criteria = criterium.split(';').map(c => c.trim()).filter(c => c);
      if (criteria.length === 0) {
        console.warn('No criteria provided');
        return null;
      }
      // Search for each criterion individually
      const results = await Promise.all(
        criteria.map(async (c) => {
          const cleanedCriterium = this.cleanCriterium(c);
          const searchResults = await this.searchLayers(c, publication_status);
          if (!searchResults || searchResults.length === 0) {
            console.warn('No results found for criterium:', cleanedCriterium, '(original:', c + ')');
            return null;
          }
          // For not_published, the response is already the specific test layer
          let selectedLayer;
          if (publication_status === 'not_published') {
            selectedLayer = searchResults.find(layer =>
              layer.name.toLowerCase().includes('tst_actueel_indicator')
            );
          } else {
            // For published, prioritize "actueel" (current) over other results
            selectedLayer = searchResults.find(layer =>
              layer.name.toLowerCase().includes('actueel') ||
              layer.name.toLowerCase().includes('current')
            );
            // If no "actueel" found, take the first result
            if (!selectedLayer) {
              selectedLayer = searchResults[0];
            }
          }
          console.log('Selected layer for', cleanedCriterium, '(original:', c + '):', selectedLayer);
          return selectedLayer;
        })
      );
      // Filter out null results (if any)
      const validResults = results.filter(r => r);
      if (validResults.length === 0) {
        console.warn('No valid results found for any criterium');
        return null;
      }
      // Build the combined URL
      const layerIds = validResults.map(r => r.id);
      const layerNames = validResults.map(r => r.name);
      const combinedUrl = this.buildViewerUrl(layerIds, layerNames);
      return {
        url: combinedUrl,
        layerNames: layerNames,
        descriptions: validResults.map(r => r.description),
      };
    } catch (error) {
      console.error('Error getting viewer URL for criterium:', criterium, error);
      return null;
    }
  },
  // Cache for viewer URLs to avoid repeated API calls
  urlCache: new Map(),
  async getCachedViewerUrl(criterium, publication_status) {
    const cacheKey = `${criterium}|${publication_status}`;
    if (this.urlCache.has(cacheKey)) {
      return this.urlCache.get(cacheKey);
    }
    const result = await this.getViewerUrlForCriterium(criterium, publication_status);
    if (result) {
      this.urlCache.set(cacheKey, result);
    }
    return result;
  },
  clearCache() {
    this.urlCache.clear();
  },
};
