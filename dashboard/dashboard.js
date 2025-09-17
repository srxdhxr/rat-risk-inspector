class RatRiskDashboard {
    constructor() {
        this.map = null;
        this.restaurants = [];
        this.selectedRestaurant = null;
        this.zipcodeData = new Map();
        this.markers = [];
        this.apiBaseUrl = 'http://localhost:8000';

        this.init();
    }

    async init() {
        this.showLoading(true);
        await this.initializeMap();
        await this.loadData();
        this.setupEventListeners();
        this.showLoading(false);
    }

    initializeMap() {
        // Initialize Leaflet map centered on NYC
        this.map = L.map('map').setView([40.7128, -74.0060], 11);

        // Add OpenStreetMap tiles
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors'
        }).addTo(this.map);
    }

    async loadData() {
        try {
            // Load restaurant data
            const response = await fetch(`${this.apiBaseUrl}/data/main_mart.fact_restaurant_rat?limit=2000`);

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            this.restaurants = data.data || [];

            // Process zipcode data for map coloring
            this.processZipcodeData();
            this.updateMap();

        } catch (error) {
            console.error('Error loading data:', error);
            this.showError('Failed to load data. Please check if the API is running.');
        }
    }

    processZipcodeData() {
        // Group restaurants by zipcode and calculate average rat activity
        const zipcodeStats = new Map();

        this.restaurants.forEach(restaurant => {
            const zipcode = restaurant.zipcode;
            if (!zipcode) return;

            if (!zipcodeStats.has(zipcode)) {
                zipcodeStats.set(zipcode, {
                    total: 0,
                    activitySum: 0,
                    count: 0,
                    restaurants: []
                });
            }

            const stats = zipcodeStats.get(zipcode);
            stats.total++;
            stats.activitySum += restaurant.zip_rat_activity_rate_6m || 0;
            stats.count++;
            stats.restaurants.push(restaurant);
        });

        // Calculate average activity rate for each zipcode
        zipcodeStats.forEach((stats, zipcode) => {
            this.zipcodeData.set(zipcode, {
                avgActivityRate: stats.activitySum / stats.count,
                restaurantCount: stats.count,
                restaurants: stats.restaurants
            });
        });
    }

    updateMap() {
        // Clear existing markers
        this.markers.forEach(marker => this.map.removeLayer(marker));
        this.markers = [];

        // Add zipcode-based coloring
        this.zipcodeData.forEach((data, zipcode) => {
            const activityRate = data.avgActivityRate;
            const color = this.getZipcodeColor(activityRate);

            // Create a circle for the zipcode area (simplified)
            data.restaurants.forEach(restaurant => {
                if (restaurant.latitude && restaurant.longitude) {
                    const marker = L.circleMarker([restaurant.latitude, restaurant.longitude], {
                        radius: 6,
                        fillColor: color,
                        color: 'white',
                        weight: 2,
                        opacity: 1,
                        fillOpacity: 0.7
                    });

                    // Add popup
                    const popupContent = this.createZipcodePopup(zipcode, data);
                    marker.bindPopup(popupContent);

                    marker.addTo(this.map);
                    this.markers.push(marker);
                }
            });
        });

        // Highlight selected restaurant if any
        if (this.selectedRestaurant) {
            this.highlightSelectedRestaurant();
        }

        // Fit map to show all markers
        if (this.markers.length > 0) {
            const group = new L.featureGroup(this.markers);
            this.map.fitBounds(group.getBounds().pad(0.1));
        }
    }

    getZipcodeColor(activityRate) {
        if (activityRate >= 0.7) return '#e53e3e'; // High activity
        if (activityRate >= 0.3) return '#f6ad55'; // Medium activity
        return '#38a169'; // Low activity
    }

    createZipcodePopup(zipcode, data) {
        return `
            <div class="popup-content">
                <h4>ZIP Code: ${zipcode}</h4>
                <p><strong>Restaurants:</strong> ${data.restaurantCount}</p>
                <p><strong>Avg Rat Activity:</strong> ${(data.avgActivityRate * 100).toFixed(1)}%</p>
            </div>
        `;
    }

    highlightSelectedRestaurant() {
        if (!this.selectedRestaurant || !this.selectedRestaurant.latitude || !this.selectedRestaurant.longitude) return;

        // Create a highlighted marker for selected restaurant
        const highlightMarker = L.circleMarker([this.selectedRestaurant.latitude, this.selectedRestaurant.longitude], {
            radius: 12,
            fillColor: '#667eea',
            color: '#4c51bf',
            weight: 3,
            opacity: 1,
            fillOpacity: 0.8
        });

        highlightMarker.addTo(this.map);
        this.markers.push(highlightMarker);

        // Center map on selected restaurant
        this.map.setView([this.selectedRestaurant.latitude, this.selectedRestaurant.longitude], 15);
    }

    setupEventListeners() {
        // Search functionality
        const searchInput = document.getElementById('restaurantSearch');
        const searchBtn = document.getElementById('searchBtn');
        const searchResults = document.getElementById('searchResults');

        let searchTimeout;

        searchInput.addEventListener('input', (e) => {
            clearTimeout(searchTimeout);
            const query = e.target.value.trim();

            if (query.length < 2) {
                searchResults.style.display = 'none';
                return;
            }

            searchTimeout = setTimeout(() => {
                this.searchRestaurants(query);
            }, 300);
        });

        searchBtn.addEventListener('click', () => {
            const query = searchInput.value.trim();
            if (query.length >= 2) {
                this.searchRestaurants(query);
            }
        });

        // Hide search results when clicking outside
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.search-container')) {
                searchResults.style.display = 'none';
            }
        });
    }

    searchRestaurants(query) {
        const results = this.restaurants.filter(restaurant =>
            restaurant.dba && restaurant.dba.toLowerCase().includes(query.toLowerCase())
        ).slice(0, 10);

        const searchResults = document.getElementById('searchResults');

        if (results.length === 0) {
            searchResults.innerHTML = '<div class="search-result-item">No restaurants found</div>';
        } else {
            searchResults.innerHTML = results.map(restaurant => {
                const score = restaurant.avg_score_6m || 0;
                const scoreClass = this.getScoreClass(score);
                const scoreText = score > 0 ? score.toFixed(1) : 'N/A';

                return `
                    <div class="search-result-item" data-camis="${restaurant.camis}">
                        <div>
                            <div class="restaurant-name">${restaurant.dba}</div>
                            <div class="restaurant-address">${restaurant.street}, ${restaurant.boro}</div>
                        </div>
                        <div class="restaurant-score ${scoreClass}">${scoreText}</div>
                    </div>
                `;
            }).join('');

            // Add click handlers
            searchResults.querySelectorAll('.search-result-item[data-camis]').forEach(item => {
                item.addEventListener('click', () => {
                    const camis = item.dataset.camis;
                    const restaurant = this.restaurants.find(r => r.camis == camis);
                    if (restaurant) {
                        this.selectRestaurant(restaurant);
                        searchResults.style.display = 'none';
                        document.getElementById('restaurantSearch').value = restaurant.dba;
                    }
                });
            });
        }

        searchResults.style.display = 'block';
    }

    getScoreClass(score) {
        if (score >= 90) return 'score-excellent';
        if (score >= 80) return 'score-good';
        if (score >= 70) return 'score-fair';
        return 'score-poor';
    }

    async selectRestaurant(restaurant) {
        this.selectedRestaurant = restaurant;
        this.showLoading(true);

        try {
            // Load detailed inspection data
            await this.loadRestaurantDetails(restaurant);
            this.showRestaurantDetails();
            this.updateMap();
        } catch (error) {
            console.error('Error loading restaurant details:', error);
            this.showError('Failed to load restaurant details');
        } finally {
            this.showLoading(false);
        }
    }

    async loadRestaurantDetails(restaurant) {
        // Load restaurant inspection data
        try {
            const inspectionResponse = await fetch(
                `${this.apiBaseUrl}/query`,
                {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        query: `SELECT * FROM main_mart.fact_restaurant_inspection WHERE camis = '${restaurant.camis}' ORDER BY inspection_date DESC LIMIT 1`
                    })
                }
            );

            if (inspectionResponse.ok) {
                const inspectionData = await inspectionResponse.json();
                this.restaurantInspection = inspectionData.data[0] || null;
            }
        } catch (error) {
            console.error('Error loading inspection data:', error);
            this.restaurantInspection = null;
        }

        // Load rat inspection data
        try {
            const ratResponse = await fetch(
                `${this.apiBaseUrl}/query`,
                {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        query: `SELECT * FROM main_mart.fact_restaurant_rat WHERE camis = '${restaurant.camis}' LIMIT 1`
                    })
                }
            );

            if (ratResponse.ok) {
                const ratData = await ratResponse.json();
                this.restaurantRatData = ratData.data[0] || null;
            }
        } catch (error) {
            console.error('Error loading rat data:', error);
            this.restaurantRatData = null;
        }
    }

    showRestaurantDetails() {
        const section = document.getElementById('restaurantDetailsSection');
        section.style.display = 'block';

        // Update restaurant inspection details
        this.updateInspectionDetails();

        // Update rat activity details
        this.updateRatDetails();
    }

    updateInspectionDetails() {
        const inspection = this.restaurantInspection;

        if (!inspection) {
            document.getElementById('latestScore').textContent = 'N/A';
            document.getElementById('inspectionDate').textContent = 'N/A';
            document.getElementById('criticalViolations').textContent = 'N/A';
            document.getElementById('highSeverity').textContent = 'N/A';
            document.getElementById('inspectionGrade').textContent = 'N/A';
            document.getElementById('violationsList').innerHTML = '<p>No inspection data available</p>';
            return;
        }

        // Update basic metrics
        document.getElementById('latestScore').textContent = inspection.average_score || 'N/A';
        document.getElementById('inspectionDate').textContent = inspection.inspection_date || 'N/A';
        document.getElementById('criticalViolations').textContent = inspection.critical_violations || 0;
        document.getElementById('highSeverity').textContent = inspection.high_severity_violation_count || 0;

        // Update grade badge
        const grade = inspection.grade || 'N';
        const gradeElement = document.getElementById('inspectionGrade');
        gradeElement.textContent = grade;
        gradeElement.className = `card-badge ${this.getGradeClass(grade)}`;

        // Update violations list
        this.updateViolationsList(inspection);
    }

    getGradeClass(grade) {
        switch (grade) {
            case 'A': return 'badge-excellent';
            case 'B': return 'badge-good';
            case 'C': return 'badge-fair';
            default: return 'badge-poor';
        }
    }

    updateViolationsList(inspection) {
        const violationsList = document.getElementById('violationsList');

        if (inspection.violation_descriptions && inspection.violation_descriptions.length > 0) {
            violationsList.innerHTML = inspection.violation_descriptions.map(desc =>
                `<div class="violation-item">${desc}</div>`
            ).join('');
        } else {
            violationsList.innerHTML = '<p>No violations recorded</p>';
        }
    }

    updateRatDetails() {
        const ratData = this.restaurantRatData;

        if (!ratData) {
            document.getElementById('ratActivityRate').textContent = 'N/A';
            document.getElementById('ratPassRate').textContent = 'N/A';
            document.getElementById('ratInspections').textContent = 'N/A';
            document.getElementById('lastRatInspection').textContent = 'N/A';
            document.getElementById('ratActivityStatus').textContent = 'N/A';
            return;
        }

        // Update metrics
        const activityRate = ratData.rat_activity_rate_6m || 0;
        const passRate = ratData.rat_pass_rate_6m || 0;

        document.getElementById('ratActivityRate').textContent = `${(activityRate * 100).toFixed(1)}%`;
        document.getElementById('ratPassRate').textContent = `${(passRate * 100).toFixed(1)}%`;
        document.getElementById('ratInspections').textContent = ratData.total_rat_inspections_6m || 'N/A';
        document.getElementById('lastRatInspection').textContent = ratData.recent_rat_insp_date || 'N/A';

        // Update status badge
        const statusElement = document.getElementById('ratActivityStatus');
        if (activityRate >= 0.5) {
            statusElement.textContent = 'ACTIVE';
            statusElement.className = 'card-badge badge-active';
        } else {
            statusElement.textContent = 'INACTIVE';
            statusElement.className = 'card-badge badge-inactive';
        }

        // Update trend
        this.updateRatTrend(ratData);
    }

    updateRatTrend(ratData) {
        const trendElement = document.getElementById('ratTrend');
        const trend = ratData.rat_activity_trend_6m;

        if (trend === null || trend === undefined) {
            trendElement.innerHTML = '<p class="trend-stable">No trend data available</p>';
            return;
        }

        let trendHtml = '';
        if (trend > 0.1) {
            trendHtml = `<p class="trend-up">📈 Activity increasing by ${(trend * 100).toFixed(1)}%</p>`;
        } else if (trend < -0.1) {
            trendHtml = `<p class="trend-down">📉 Activity decreasing by ${Math.abs(trend * 100).toFixed(1)}%</p>`;
        } else {
            trendHtml = `<p class="trend-stable">➡️ Activity stable</p>`;
        }

        trendElement.innerHTML = trendHtml;
    }

    showLoading(show) {
        const overlay = document.getElementById('loadingOverlay');
        if (show) {
            overlay.classList.remove('hidden');
        } else {
            overlay.classList.add('hidden');
        }
    }

    showError(message) {
        console.error(message);
        // You could add a toast notification here
    }
}

// Initialize dashboard when page loads
document.addEventListener('DOMContentLoaded', () => {
    new RatRiskDashboard();
});
