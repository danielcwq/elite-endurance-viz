from fasthtml.common import *
import json
import pandas as pd
from typing import Optional, Dict
from abc import ABC, abstractmethod
from urllib.parse import unquote
from mongodb_init.connection import DatabaseConnection
from utils.helpers import mongo_to_json_serializable, MongoJSONEncoder

from datetime import datetime

def parse_latest_log_stats():
    try:
        # Get the latest log entry from MongoDB
        latest_log = db.db.update_logs.find_one(
            sort=[('timestamp', -1)]  # Sort by timestamp descending
        )
        
        if latest_log:
            return {
                'last_updated': latest_log['timestamp'].strftime('%Y-%m-%d %H:%M:%S') + ' (GMT +8)',
                'total_activities': latest_log['activities_count']
            }
        
        return {
            'last_updated': 'Unknown',
            'total_activities': 0
        }
    except Exception as e:
        print(f"Error parsing MongoDB log: {e}")
        return {
            'last_updated': 'Unknown',
            'total_activities': 0
        }

def format_date(date_str):
    """Format date string into readable format"""
    if not date_str:
        return "-"
    
    try:
        # Handle the specific format you're getting
        if '+00:00' in date_str:
            # Remove the timezone part and parse
            clean_date = date_str.replace('+00:00', '')
            date_obj = datetime.strptime(clean_date, '%Y-%m-%d %H:%M:%S')
        else:
            # Try other common formats
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            except ValueError:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        
        # Format to a more readable string
        return date_obj.strftime('%B %d, %Y at %I:%M %p')  # Example: "November 8, 2024 at 5:07 PM"
        
    except Exception as e:
        print(f"Error formatting date {date_str}: {str(e)}")
        return date_str


# Data Management
class DataSource(ABC):
    @abstractmethod
    def load_data(self) -> pd.DataFrame:
        pass

def _init_country_coordinates():
    return {
        'AFG': {'lat': 33.9391, 'lng': 67.7100},  # Afghanistan
        'ALB': {'lat': 41.1533, 'lng': 20.1683},  # Albania
        'ALG': {'lat': 36.7538, 'lng': 3.0588},   # Algeria
        'AND': {'lat': 42.5063, 'lng': 1.5218},   # Andorra
        'AGO': {'lat': -11.2027, 'lng': 17.8739}, # Angola
        'ARG': {'lat': -38.4161, 'lng': -63.6167}, # Argentina
        'ARM': {'lat': 40.0691, 'lng': 45.0382},  # Armenia
        'AUS': {'lat': -25.2744, 'lng': 133.7751}, # Australia
        'AUT': {'lat': 47.5162, 'lng': 14.5501},  # Austria
        'AZE': {'lat': 40.1431, 'lng': 47.5769},  # Azerbaijan
        'BHR': {'lat': 26.0667, 'lng': 50.5577},  # Bahrain
        'BGD': {'lat': 23.6850, 'lng': 90.3563},  # Bangladesh
        'BLR': {'lat': 53.7098, 'lng': 27.9534},  # Belarus
        'BEL': {'lat': 50.8503, 'lng': 4.3517},   # Belgium
        'BEN': {'lat': 9.3077, 'lng': 2.3158},    # Benin
        'BTN': {'lat': 27.5142, 'lng': 90.4336},  # Bhutan
        'BOL': {'lat': -16.2902, 'lng': -63.5887}, # Bolivia
        'BIH': {'lat': 43.9159, 'lng': 17.6791},  # Bosnia and Herzegovina
        'BWA': {'lat': -22.3285, 'lng': 24.6849}, # Botswana
        'BRA': {'lat': -14.2350, 'lng': -51.9253}, # Brazil
        'BRN': {'lat': 4.5353, 'lng': 114.7277},  # Brunei
        'BGR': {'lat': 42.7339, 'lng': 25.4858},  # Bulgaria
        'BFA': {'lat': 12.2383, 'lng': -1.5616},  # Burkina Faso
        'BDI': {'lat': -3.3731, 'lng': 29.9189},  # Burundi
        'KHM': {'lat': 12.5657, 'lng': 104.9910}, # Cambodia
        'CMR': {'lat': 7.3697, 'lng': 12.3547},   # Cameroon
        'CAN': {'lat': 56.1304, 'lng': -106.3468}, # Canada
        'CHN': {'lat': 35.8617, 'lng': 104.1954}, # China
        'COL': {'lat': 4.5709, 'lng': -74.2973},  # Colombia
        'CRI': {'lat': 9.7489, 'lng': -83.7534},  # Costa Rica
        'HRV': {'lat': 45.1000, 'lng': 15.2000},  # Croatia
        'CUB': {'lat': 21.5218, 'lng': -77.7812}, # Cuba
        'CYP': {'lat': 35.1264, 'lng': 33.4299},  # Cyprus
        'CZE': {'lat': 49.8175, 'lng': 15.4730},  # Czech Republic
        'DEN': {'lat': 56.2639, 'lng': 9.5018},   # Denmark
        'DJI': {'lat': 11.8251, 'lng': 42.5903},  # Djibouti
        'DOM': {'lat': 18.7357, 'lng': -70.1627}, # Dominican Republic
        'ECU': {'lat': -1.8312, 'lng': -78.1834}, # Ecuador
        'EGY': {'lat': 26.8206, 'lng': 30.8025},  # Egypt
        'ERI': {'lat': 15.1794, 'lng': 39.7823},  # Eritrea
        'EST': {'lat': 58.5953, 'lng': 25.0136},  # Estonia
        'ETH': {'lat': 9.1450, 'lng': 40.4897},   # Ethiopia
        'FJI': {'lat': -17.7134, 'lng': 178.0650}, # Fiji
        'FIN': {'lat': 61.9241, 'lng': 25.7482},  # Finland
        'FRA': {'lat': 46.2276, 'lng': 2.2137},   # France
        'GAB': {'lat': -0.8037, 'lng': 11.6094},  # Gabon
        'GEO': {'lat': 42.3154, 'lng': 43.3569},  # Georgia
        'GER': {'lat': 51.1657, 'lng': 10.4515},  # Germany
        'GHA': {'lat': 7.9465, 'lng': -1.0232},   # Ghana
        'GRC': {'lat': 39.0742, 'lng': 21.8243},  # Greece
        'GTM': {'lat': 15.7835, 'lng': -90.2308}, # Guatemala
        'GIN': {'lat': 9.9456, 'lng': -9.6966},   # Guinea
        'GUY': {'lat': 4.8604, 'lng': -58.9302},  # Guyana
        'HTI': {'lat': 18.9712, 'lng': -72.2852}, # Haiti
        'HND': {'lat': 15.1999, 'lng': -86.2419}, # Honduras
        'HUN': {'lat': 47.1625, 'lng': 19.5033},  # Hungary
        'ISL': {'lat': 64.9631, 'lng': -19.0208}, # Iceland
        'IND': {'lat': 20.5937, 'lng': 78.9629},  # India
        'IDN': {'lat': -0.7893, 'lng': 113.9213}, # Indonesia
        'IRN': {'lat': 32.4279, 'lng': 53.6880},  # Iran
        'IRQ': {'lat': 33.2232, 'lng': 43.6793},  # Iraq
        'IRL': {'lat': 53.1424, 'lng': -7.6921},  # Ireland
        'ISR': {'lat': 31.0461, 'lng': 34.8516},  # Israel
        'ITA': {'lat': 41.8719, 'lng': 12.5674},  # Italy
        'JAM': {'lat': 18.1096, 'lng': -77.2975}, # Jamaica
        'JPN': {'lat': 36.2048, 'lng': 138.2529}, # Japan
        'JOR': {'lat': 30.5852, 'lng': 36.2384},  # Jordan
        'KAZ': {'lat': 48.0196, 'lng': 66.9237},  # Kazakhstan
        'KEN': {'lat': -0.0236, 'lng': 37.9062},  # Kenya
        'KWT': {'lat': 29.3117, 'lng': 47.4818},  # Kuwait
        'KGZ': {'lat': 41.2044, 'lng': 74.7661},  # Kyrgyzstan
        'LAO': {'lat': 19.8563, 'lng': 102.4955}, # Laos
        'LVA': {'lat': 56.8796, 'lng': 24.6032},  # Latvia
        'LBN': {'lat': 33.8547, 'lng': 35.8623},  # Lebanon
        'LBR': {'lat': 6.4281, 'lng': -9.4295},   # Liberia
        'LBY': {'lat': 26.3351, 'lng': 17.2283},  # Libya
        'LIE': {'lat': 47.1660, 'lng': 9.5554},   # Liechtenstein
        'LTU': {'lat': 55.1694, 'lng': 23.8813},  # Lithuania
        'LUX': {'lat': 49.8153, 'lng': 6.1296},   # Luxembourg
        'MKD': {'lat': 41.6086, 'lng': 21.7453},  # North Macedonia
        'MDG': {'lat': -18.7669, 'lng': 46.8691}, # Madagascar
        'MWI': {'lat': -13.2543, 'lng': 34.3015}, # Malawi
        'MYS': {'lat': 4.2105, 'lng': 101.9758},  # Malaysia
        'MLI': {'lat': 17.5707, 'lng': -3.9962},  # Mali
        'MLT': {'lat': 35.9375, 'lng': 14.3754},  # Malta
        'MRT': {'lat': 21.0079, 'lng': -10.9408}, # Mauritania
        'MEX': {'lat': 23.6345, 'lng': -102.5528}, # Mexico
        'MDA': {'lat': 47.4116, 'lng': 28.3699},  # Moldova
        'MCO': {'lat': 43.7384, 'lng': 7.4246},   # Monaco
        'MNG': {'lat': 46.8625, 'lng': 103.8467}, # Mongolia
        'MNE': {'lat': 42.7087, 'lng': 19.3744},  # Montenegro
        'MAR': {'lat': 31.7917, 'lng': -7.0926},  # Morocco
        'MOZ': {'lat': -18.6657, 'lng': 35.5296}, # Mozambique
        'MMR': {'lat': 21.9162, 'lng': 95.9560},  # Myanmar
        'NAM': {'lat': -22.9576, 'lng': 18.4904}, # Namibia
        'NPL': {'lat': 28.3949, 'lng': 84.1240},  # Nepal
        'NED': {'lat': 52.1326, 'lng': 5.2913},   # Netherlands
        'NZL': {'lat': -40.9006, 'lng': 174.8860}, # New Zealand
        'NIC': {'lat': 12.8654, 'lng': -85.2072}, # Nicaragua
        'NER': {'lat': 17.6078, 'lng': 8.0817},   # Niger
        'NGA': {'lat': 9.0820, 'lng': 8.6753},    # Nigeria
        'PRK': {'lat': 40.3399, 'lng': 127.5101}, # North Korea
        'NOR': {'lat': 60.4720, 'lng': 8.4689},   # Norway
        'OMN': {'lat': 21.4735, 'lng': 55.9754},  # Oman
        'PAK': {'lat': 30.3753, 'lng': 69.3451},  # Pakistan
        'PAN': {'lat': 8.5380, 'lng': -80.7821},  # Panama
        'PNG': {'lat': -6.3150, 'lng': 143.9555}, # Papua New Guinea
        'PRY': {'lat': -23.4425, 'lng': -58.4438}, # Paraguay
        'PER': {'lat': -9.1900, 'lng': -75.0152}, # Peru
        'PHL': {'lat': 12.8797, 'lng': 121.7740}, # Philippines
        'POL': {'lat': 51.9194, 'lng': 19.1451},  # Poland
        'POR': {'lat': 39.3999, 'lng': -8.2245},  # Portugal
        'QAT': {'lat': 25.3548, 'lng': 51.1839},  # Qatar
        'ROU': {'lat': 45.9432, 'lng': 24.9668},  # Romania
        'RUS': {'lat': 61.5240, 'lng': 105.3188}, # Russia
        'RWA': {'lat': -1.9403, 'lng': 29.8739},  # Rwanda
        'SAU': {'lat': 23.8859, 'lng': 45.0792},  # Saudi Arabia
        'SEN': {'lat': 14.4974, 'lng': -14.4524}, # Senegal
        'SRB': {'lat': 44.0165, 'lng': 21.0059},  # Serbia
        'SGP': {'lat': 1.3521, 'lng': 103.8198},  # Singapore
        'SVK': {'lat': 48.6690, 'lng': 19.6990},  # Slovakia
        'SVN': {'lat': 46.1512, 'lng': 14.9955},  # Slovenia
        'SOM': {'lat': 5.1521, 'lng': 46.1996},   # Somalia
        'RSA': {'lat': -30.5595, 'lng': 22.9375}, # South Africa
        'KOR': {'lat': 35.9078, 'lng': 127.7669}, # South Korea
        'SSD': {'lat': 6.8770, 'lng': 31.3070},   # South Sudan
        'ESP': {'lat': 40.4637, 'lng': -3.7492},  # Spain
        'LKA': {'lat': 7.8731, 'lng': 80.7718},   # Sri Lanka
        'SDN': {'lat': 12.8628, 'lng': 30.2176},  # Sudan
        'SWE': {'lat': 60.1282, 'lng': 18.6435},  # Sweden
        'SUI': {'lat': 46.8182, 'lng': 8.2275},   # Switzerland
        'SYR': {'lat': 34.8021, 'lng': 38.9968},  # Syria
        'TWN': {'lat': 23.5937, 'lng': 121.0254}, # Taiwan
        'TJK': {'lat': 38.8610, 'lng': 71.2761},  # Tajikistan
        'TZA': {'lat': -6.3690, 'lng': 34.8888},  # Tanzania
        'THA': {'lat': 15.8700, 'lng': 100.9925}, # Thailand
        'TGO': {'lat': 8.6195, 'lng': 0.8248},    # Togo
        'TUN': {'lat': 33.8869, 'lng': 9.5375},   # Tunisia
        'TUR': {'lat': 38.9637, 'lng': 35.2433},  # Turkey
        'TKM': {'lat': 38.9697, 'lng': 59.5563},  # Turkmenistan
        'UGA': {'lat': 1.3733, 'lng': 32.2903},   # Uganda
        'UKR': {'lat': 48.3794, 'lng': 31.1656},  # Ukraine
        'ARE': {'lat': 23.4241, 'lng': 53.8478},  # United Arab Emirates
        'GBR': {'lat': 55.3781, 'lng': -3.4360},  # United Kingdom
        'USA': {'lat': 37.0902, 'lng': -95.7129}, # United States
        'URU': {'lat': -32.5228, 'lng': -55.7658}, # Uruguay
        'UZB': {'lat': 41.3775, 'lng': 64.5853},  # Uzbekistan
        'VEN': {'lat': 6.4238, 'lng': -66.5897},  # Venezuela
        'VNM': {'lat': 14.0583, 'lng': 108.2772}, # Vietnam
        'YEM': {'lat': 15.5527, 'lng': 48.5164},  # Yemen
        'ZMB': {'lat': -13.1339, 'lng': 27.8493}, # Zambia
        'ZWE': {'lat': -19.0154, 'lng': 29.1549}, # Zimbabwe
    }


# FastHTML App
app, rt = fast_app(
    use_sessions=False,
    hdrs=(
        Link(rel = 'stylesheet', href = 'https://cdn.jsdelivr.net/npm/@picocss/pico@1/css/pico.min.css'),
        Link(rel='stylesheet', href='https://unpkg.com/leaflet@1.7.1/dist/leaflet.css'),
        Link(rel='stylesheet', href='https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.css'),
        Link(rel='stylesheet', href='https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.Default.css'),
        Script(src='https://unpkg.com/leaflet@1.7.1/dist/leaflet.js'),
        Script(src='https://unpkg.com/leaflet.markercluster@1.4.1/dist/leaflet.markercluster.js'),
        Script("""
            <!-- Vercel Analytics -->
            <script>
                window.va = window.va || function () { (window.vaq = window.vaq || []).push(arguments); };
            </script>
            <script defer src="/_vercel/insights/script.js"></script>
        """),
    )
)

db = DatabaseConnection.get_instance()


def create_map_script(athletes_data: list) -> str:
    return f"""
        var map = L.map('map').setView([0, 0], 2);
        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '© OpenStreetMap contributors'
        }}).addTo(map);

        var athletes = {json.dumps(athletes_data, cls=MongoJSONEncoder)};
        var markers = L.markerClusterGroup({{
            maxClusterRadius: 30,
            spiderfyOnMaxZoom: true,
            showCoverageOnHover: true,
            zoomToBoundsOnClick: true
        }});

        var locationGroups = {{}};
        athletes.forEach(function(athlete) {{
            if (athlete.latitude && athlete.longitude) {{
                var key = athlete.latitude + ',' + athlete.longitude;
                if (!locationGroups[key]) locationGroups[key] = [];
                locationGroups[key].push(athlete);
            }}
        }});

        Object.keys(locationGroups).forEach(function(key) {{
            var athletes = locationGroups[key];
            var [lat, lng] = key.split(',');
            var popupContent = '<div style="max-height: 200px; overflow-y: auto; font-size: 11px;">' +  // Reduced base font size
                '<h3 style="font-size: 12px; margin: 0 0 4px 0;">' + athletes[0].Nat + '</h3>' +  // Further reduced h3
                '<p style="margin: 0 0 4px 0; font-size: 10px;">Athletes: ' + athletes.length + '</p>' +  // Smaller count text
                '<ul style="margin: 0; padding-left: 12px; font-size: 10px;">' +  // Smaller list font
                athletes.map(function(a) {{
                    return '<li style="margin-bottom: 2px;">' +  // Reduced margin
                           '<a href="/athlete/' + encodeURIComponent(a.Competitor) + 
                           '" style="text-decoration: none; font-size: 10px;">' +  // Explicit font size for names
                           a['Athlete Name'] + 
                           '</a> ' +
                           '<span style="color: #666; font-size: 9px;">(' + a.Discipline + ')</span></li>';  // Even smaller discipline text
                }}).join('') +
                '</ul></div>';
            
            L.marker([parseFloat(lat), parseFloat(lng)])
             .bindPopup(popupContent, {{
                maxWidth: 200,  // Reduced popup width
                autoPanPadding: [50, 50]
             }})
             .addTo(markers);
        }});

        map.addLayer(markers);

        map.on('zoomend', function() {{
            var currentZoom = map.getZoom();
            markers.options.maxClusterRadius = currentZoom < 5 ? 30 : 10;
            markers.refreshClusters();
        }});
    """
def get_analytics_script(measurement_id: str = 'G-TFWZT8GQTN') -> Script:
    return Script("""
        <!-- Google tag (gtag.js) -->
        <script async src="https://www.googletagmanager.com/gtag/js?id=G-TFWZT8GQTN"></script>
        <script>
          window.dataLayer = window.dataLayer || [];
          function gtag(){dataLayer.push(arguments);}
          gtag('js', new Date());
        
          gtag('config', 'G-TFWZT8GQTN');
        </script>
    """)


@rt("/")
def get():
    athletes_data = mongo_to_json_serializable(list(db.db.athlete_metadata.find({})))
    total_athletes = len(athletes_data)
    total_countries = len(set(athlete['Nat'] for athlete in athletes_data))
    log_stats = parse_latest_log_stats()
    country_coords = _init_country_coordinates()
    for athlete in athletes_data:
        country_code = athlete.get('Nat')
        if country_code and country_code in country_coords:
            athlete['latitude'] = country_coords[country_code]['lat']
            athlete['longitude'] = country_coords[country_code]['lng']
    return (
        get_analytics_script(),
        Style("""
            /* Only custom styles that Pico doesn't provide */
            .site-title {
                text-align: center;
                margin: var(--typography-spacing-vertical) 0;
            }

            .search-container {
                max-width: 800px;
                margin: 0 auto;
                position: relative; 
            }

            /* Search results styling - minimal custom CSS */
            .search-results {
                position: absolute;
                top: 100%;         /* Position below the search container */
                left: 0;
                right: 0;          /* Stretch to container width */
                max-height: 300px;
                overflow-y: auto;
                background: var(--card-background-color);
                border: var(--border-width) solid var(--card-border-color);
                border-radius: var(--border-radius);
                margin-top: 0.5rem;
                box-shadow: var(--card-box-shadow);
                display: none;
                z-index: 1000;     /* Ensure it appears above other content */
            }

            .search-results.active {
                display: block;
            }

            /* Make search result items more compact */
            .search-results article {
                padding: var(--spacing-small);  /* Use Pico's small spacing */
                margin: 0;  /* Remove default article margins */
                border-bottom: var(--border-width) solid var(--card-border-color);
            }

            .search-results article:last-child {
                border-bottom: none;
            }

            .search-results article:hover {
                background: var(--card-sectionning-background-color);
            }

            /* Hide map button on mobile */
            @media (max-width: 576px) {
                .map-view-btn {
                    display: none;
                }
            }
            .stats-container {
                max-width: 800px;
                margin: 2rem auto 0;
            }

            /* Toggle buttons container */
            .toggle-container {
                display: flex;
                gap: var(--spacing);
                margin-bottom: var(--spacing);
            }

            /* Hide sections by default */
            .section {
                display: none;
            }

            .section.active {
                display: block;
            }
        """),
        Main(
            Header(
                H1("Elite Runners Database", cls="site-title"),
                cls="container-fluid"
            ),
            Div(
                Div(
                    Div(
                        # Add id to search input
                        Input(
                            type="search", 
                            placeholder="Search athletes...",
                            id="athlete-search",
                            cls="search-input"
                        ),
                        Button(
                            "Map View", 
                            cls="map-view-btn contrast",
                            style="width: auto;"
                        ),
                        Div(cls="search-results", id="search-results"),
                        cls="grid"
                    ),
                    cls="search-container"
                ),
                Div(
                    id = "map",
                    style="height: 400px; width: 100%; display: none; margin: var(--spacing) 0;"
                ),
                cls="container"
            ),
                Script(f"""
                // Get DOM elements
                const searchInput = document.getElementById('athlete-search');
                const searchResults = document.getElementById('search-results');
                const mapBtn = document.querySelector('.map-view-btn');
                const mapContainer = document.getElementById('map');
                
                // Initialize data first
                var athletes = {json.dumps(athletes_data, cls=MongoJSONEncoder)};
                
                // Map state tracking
                let mapInitialized = false;
                
                // Debounce function for search
                function debounce(func, wait) {{
                    let timeout;
                    return function executedFunction(...args) {{
                        const later = () => {{
                            clearTimeout(timeout);
                            func(...args);
                        }};
                        clearTimeout(timeout);
                        timeout = setTimeout(later, wait);
                    }};
                }}
                
                // Search functionality
                const performSearch = debounce((query) => {{
                    if (!query) {{
                        searchResults.innerHTML = '';
                        searchResults.classList.remove('active');
                        return;
                    }}
                    
                    const filteredAthletes = athletes  // Now athletes is defined before this is called
                        .filter(athlete => 
                            athlete['Athlete Name'].toLowerCase().includes(query.toLowerCase())
                        )
                        .slice(0, 10);
                    
                    const resultsHTML = filteredAthletes.length 
                        ? filteredAthletes
                            .map(athlete => `
                                <article>
                                    <a href="/athlete/${{encodeURIComponent(athlete.Competitor)}}" 
                                    style="text-decoration: none;">
                                        <div style="margin: 0;">
                                            <strong>${{athlete['Athlete Name']}}</strong>
                                            <small style="display: block; color: var(--muted-color);">
                                                ${{athlete.Nat}} • ${{athlete.Discipline}}
                                            </small>
                                        </div>
                                    </a>
                                </article>
                            `)
                            .join('')
                        : '<article><p>No athletes found</p></article>';
                    
                    searchResults.innerHTML = resultsHTML;
                    searchResults.classList.add('active');
                }}, 300);
                
                // Map toggle functionality
                mapBtn.addEventListener('click', function() {{
                    const isMapVisible = mapContainer.style.display !== 'none';
                    mapContainer.style.display = isMapVisible ? 'none' : 'block';
                    
                    // Initialize map on first show
                    if (!isMapVisible && !mapInitialized) {{
                        mapInitialized = true;
                        {create_map_script(athletes_data)}
                    }}
                }});
                
                // Search event listeners
                searchInput.addEventListener('input', (e) => performSearch(e.target.value));
                
                // Close results when clicking outside
                document.addEventListener('click', (e) => {{
                    if (!searchResults.contains(e.target) && e.target !== searchInput) {{
                        searchResults.classList.remove('active');
                    }}
                }});
                
                // Prevent clicks within results from closing
                searchResults.addEventListener('click', (e) => {{
                    e.stopPropagation();
                }});
            """),
            Div(
                Div(
                    Article(
                        # Toggle buttons inside the Article
                        Div(
                            Button(
                                "Stats", 
                                id="stats-btn",
                                cls="outline",
                                onclick="toggleSection('stats')"
                            ),
                            Button(
                                "About", 
                                id="about-btn",
                                cls="outline",
                                onclick="toggleSection('about')"
                            ),
                            cls="toggle-container"
                        ),
                        # Stats section
                        Div(
                            Div(
                                Div(
                                    P("Total Athletes: ", style="margin: 0; display: inline;"),
                                    P(str(total_athletes), 
                                    style="font-family: var(--font-family-monospace); margin: 0; display: inline;"),
                                ),
                                Div(
                                    P("Total Countries: ", style="margin: 0; display: inline;"),
                                    P(str(total_countries), 
                                    style="font-family: var(--font-family-monospace); margin: 0; display: inline;"),
                                    style="margin-top: var(--spacing);"
                                ),
                                Div(
                                P("Total Activities: ", style="margin: 0; display: inline;"),
                                P(f"{log_stats['total_activities']:,}", 
                                  style="font-family: var(--font-family-monospace); margin: 0; display: inline;"),
                                style="margin-top: var(--spacing);"
                                ),
                                Div(
                                    P("Last Updated: ", style="margin: 0; display: inline;"),
                                    P(log_stats['last_updated'], 
                                    style="font-family: var(--font-family-monospace); margin: 0; display: inline;"),
                                    style="margin-top: var(--spacing);"
                                ),
                            ),
                            id="stats-section",
                            cls="section active"
                        ),
                        # About section
                        Div(
                            H3("About this project"),
                            Ul(
                                Li("""
                                    A visualisation of elite runners' training metadata based on 
                                    2024 public Strava data.
                                """),
                                Li("""
                                    Elite runners are arbitrarily given a cutoff of >= 1100 
                                    IAAF points.
                                """),
                                Li("""
                                    Some athletes' training data might be inaccurate due to exact name matches on Strava accounts, am working on that!
                                   """),
                                Li(
                                    "Source code ",
                                    A(
                                        "here",
                                        href="https://github.com/danielcwq/elite-endurance-viz",
                                        target="_blank",
                                        style="text-decoration: none;"
                                    )
                                ),
                                Li(
                                    "Built by ",
                                    A(
                                        "Daniel Ching",
                                        href="https://danielching.me",
                                        target="_blank",
                                        style="text-decoration: none;"
                                    )
                                ),
                                style="list-style-type: disc; padding-left: var(--spacing);"  # Using Pico's spacing variable
                            ),
                            id="about-section",
                            cls="section"
                        ),
                        style="padding: var(--spacing); background: var(--card-background-color); border-radius: var(--border-radius);"
                    ),
                    cls="stats-container",
                    style="margin-top: var(--spacing);"
                ),
                cls="container"
            ),
            Script("""
            function toggleSection(section) {
                // Update buttons
                document.getElementById('stats-btn').classList.toggle('outline', section !== 'stats');
                document.getElementById('about-btn').classList.toggle('outline', section !== 'about');
                
                // Update sections
                document.getElementById('stats-section').classList.toggle('active', section === 'stats');
                document.getElementById('about-section').classList.toggle('active', section === 'about');
            }

            // Initialize with Stats active
            toggleSection('stats');
        """)
            
        )
    )

@rt("/athlete/{name}")
def get_athlete(name: str):
    # URL decode the name parameter
    decoded_name = unquote(name)
    
    athlete = db.get_athlete_metadata(decoded_name)
    if not athlete:
        return "Athlete not found", 404
    
    athlete_activities = db.get_athlete_activities(decoded_name)
    
    athlete_strava_id = athlete_activities[0].get('Athlete ID') if athlete_activities else None
    
    # Process marks and disciplines
    marks = athlete.get('Mark', '').split('|') if athlete.get('Mark') else []
    disciplines = athlete.get('Discipline', '').split('|') if athlete.get('Discipline') else []
    event_times = list(zip(disciplines, marks)) if marks and disciplines else []

    return Titled(f"{decoded_name} - Elite Runners Database",
        get_analytics_script(),
        Style("""
            /* Custom styles on top of Pico */
            .event-times {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: var(--spacing);
                margin: var(--spacing) 0;
            }

            .event-card {
                background: var(--card-background-color);
                padding: var(--spacing);
                border-radius: var(--border-radius);
                border: var(--border-width) solid var(--card-border-color);
            }

            .event-name {
                font-weight: 600;
                color: var(--color);
                margin-bottom: var(--spacing);
            }

            .event-time {
                color: var(--muted-color);
                font-size: 0.9rem;
            }

            .athlete-stats {
                margin: var(--spacing) 0;
            }
            
            .stat-card {
                background: var(--card-background-color);
                padding: var(--spacing);
                border-radius: var(--border-radius);
                box-shadow: var(--card-box-shadow);
            }
            
            .stat-value {
                font-size: 1.5rem;
                font-weight: 600;
                color: var(--color);
            }
            
            .stat-label {
                font-size: 0.875rem;
                color: var(--muted-color);
                margin-top: calc(var(--spacing) * 0.5);
            }

            .back-link {
                display: inline-block;
                margin-bottom: var(--spacing);
                color: var(--primary);
                text-decoration: none;
            }
            
            .back-link:hover {
                text-decoration: underline;
            }

            .description-box {
                display: none;  /* Hide by default */
                margin-top: var(--spacing);
                padding: var(--spacing);
                background: var(--card-sectionning-background-color);
                border-radius: var(--border-radius);
            }

            .description-box.show-description {
                display: block;  /* Show when class is added */
            }

            /* Style for clickable emoji */
            .activity-emoji {
                cursor: pointer;
                user-select: none;
            }
        """),
        Script("""
            function toggleDescription(event, id) {
                event.stopPropagation();  // Prevent event bubbling
                const descBox = document.getElementById('desc-' + id);
                
                // Close all other open descriptions
                document.querySelectorAll('.description-box').forEach(box => {
                    if (box.id !== 'desc-' + id) {
                        box.classList.remove('show-description');
                    }
                });
                
                // Toggle current description
                descBox.classList.toggle('show-description');
            }

            // Close all descriptions when clicking outside
            document.addEventListener('click', () => {
                document.querySelectorAll('.description-box').forEach(box => {
                    box.classList.remove('show-description');
                });
            });
        """),
        Main(
            Div(
                A("← Back to Map", href="/", cls="back-link"),
                H1(
                    A(
                        decoded_name,
                        href=f"https://strava.com/athletes/{athlete_strava_id}" if athlete_strava_id else None,
                        cls="strava-link",
                        target="_blank"
                    ) if athlete_strava_id else decoded_name,
                    cls="athlete-header"
                ),
                # Stats section using Pico's grid
                Div(
                    Div(
                        Div(f"{athlete.get('Total_Run_Distance_km', 0):.2f} km", cls="stat-value"),
                        Div("Total Distance", cls="stat-label"),
                        cls="stat-card"
                    ),
                    Div(
                        Div(f"{athlete.get('Total_Run_Hours', 0):.1f} hrs", cls="stat-value"),
                        Div("Total Run Hours", cls="stat-label"),
                        cls="stat-card"
                    ),
                    Div(
                        Div(f"{athlete.get('Avg_Weekly_Run_Mileage_km', 0):.1f} km", cls="stat-value"),
                        Div("Avg Weekly Mileage", cls="stat-label"),
                        cls="stat-card"
                    ),
                    Div(
                        Div(f"{athlete.get('Avg_Run_Pace_min_per_km', 0):.2f} min/km", cls="stat-value"),
                        Div("Average Pace", cls="stat-label"),
                        cls="stat-card"
                    ),
                    cls="grid athlete-stats"
                ),
                H2("Recent Activities"),
                Table(
                    Tr(
                        Th("Date"),
                        Th("Activity Name"),
                        Th("Type"),
                        Th("Distance (km)"),
                        Th("Time (min)"),
                        Th("Pace (min/km)"),
                    ),
                   *[Tr(
                        Td(format_date(row.get('Start Date'))),
                        Td(
                            Div(
                                Div(
                                    A(
                                        row.get('Activity Name'),
                                        href=f"https://strava.com/activities/{row.get('Activity ID')}" if row.get('Activity ID') else None,
                                        cls="strava-link",
                                        target="_blank"
                                    ) if row.get('Activity ID') else row.get('Activity Name'),
                                    # Fix: Check if description exists and is a string before using strip()
                                    Span(" 🔽", cls="dropdown-trigger", onclick=f"toggleDescription(event, {i})")
                                    if isinstance(row.get('Description'), str) and row.get('Description', '').strip() else "",
                                    cls="activity-name"
                                ),
                                # Fix: Check if description exists and is a string before using strip()
                                Div(row.get('Description', ''), cls="description-box", id=f"desc-{i}")
                                if isinstance(row.get('Description'), str) and row.get('Description', '').strip() else "",
                                cls="activity-cell"
                            ) if row.get('Activity Name') else "-"
                        ),
                        Td(row.get('Type')),
                        Td(f"{row.get('Distance (km)', 0):.2f}"),
                        Td(f"{row.get('Time (min)', 0):.1f}"),
                        Td(f"{row.get('Pace (min/km)', 0):.2f}"),
                    ) for i, row in enumerate(athlete_activities)],
                    cls="activities-table"
                ),
                cls="container"
            )
        )
    )

if __name__ == "__main__":
    serve(host='0.0.0.0', port=8000)
export = app