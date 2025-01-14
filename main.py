from fasthtml.common import *
import json
import pandas as pd
from typing import Optional, Dict
from abc import ABC, abstractmethod
from urllib.parse import unquote
from database.connection import DatabaseConnection
from api.routes import setup_routes
from utils.helpers import mongo_to_json_serializable, MongoJSONEncoder


# Data Management
class DataSource(ABC):
    @abstractmethod
    def load_data(self) -> pd.DataFrame:
        pass
class IndividualActivitiesDataSource(DataSource):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def load_data(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.file_path)
            # Convert Start Date to datetime
            df['Start Date'] = pd.to_datetime(df['Start Date'])
            return df
        except Exception as e:
            print(f"Error loading individual activities data: {e}")
            return pd.DataFrame()
        
class CSVDataSource(DataSource):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def load_data(self) -> pd.DataFrame:
        try:
            return pd.read_csv(self.file_path)
        except Exception as e:
            print(f"Error loading CSV data: {e}")
            return pd.DataFrame()
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
class DataManager:
    def __init__(self, data_source: DataSource):
        self.data_source = data_source
        self._country_coordinates = _init_country_coordinates()

    def load_data(self) -> pd.DataFrame:
        df = self.data_source.load_data()
        return self._enrich_data(df)

    def _enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add latitude and longitude based on country codes"""
        # Print unique countries that don't have coordinates for debugging
        missing_countries = set(df['Nat'].unique()) - set(self._country_coordinates.keys())
        if missing_countries:
            print(f"Warning: Missing coordinates for countries: {missing_countries}")
        
        # Create separate latitude and longitude columns directly
        df['latitude'] = df['Nat'].map(lambda x: self._country_coordinates.get(x, {}).get('lat'))
        df['longitude'] = df['Nat'].map(lambda x: self._country_coordinates.get(x, {}).get('lng'))
        return df

# FastHTML App
app, rt = fast_app(
    use_sessions=False,
    hdrs=(
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
setup_routes(rt)

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
            var popupContent = '<div style="max-height: 200px; overflow-y: auto;">' +
                '<h3>' + athletes[0].Nat + '</h3>' +
                '<p>Athletes: ' + athletes.length + '</p>' +
                '<ul>' +
                athletes.map(function(a) {{
                    return '<li><a href="/athlete/' + encodeURIComponent(a.Competitor) + '">' + 
                           a['Athlete Name'] + '</a> (' + a.Discipline + ')</li>';
                }}).join('') +
                '</ul></div>';
            
            L.marker([parseFloat(lat), parseFloat(lng)])
             .bindPopup(popupContent)
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
    # Initialize data
    #data_source = CSVDataSource('cleaned_athlete_metadata.csv')
    #data_manager = DataManager(data_source)
    #athletes_data = data_manager.load_data().to_dict('records')
    country_coordinates = _init_country_coordinates()
    athletes_data = mongo_to_json_serializable(list(db.db.athlete_metadata.find({})))
    for athlete in athletes_data:
        country_code = athlete.get('Nat')
        if country_code in country_coordinates:
            athlete['latitude'] = country_coordinates[country_code]['lat']
            athlete['longitude'] = country_coordinates[country_code]['lng']

    return Titled("Elite Runners Database",
        get_analytics_script(),
        Style("""
            /* Reset defaults */
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }

            body { 
                margin: 0; 
                padding: 0; 
                font-family: system-ui, -apple-system, sans-serif;
                background: #f8fafc;
                color: #1e293b;
                line-height: 1.5;
                min-height: 100vh;
                display: flex;
                flex-direction: column;
            }

            .header {
                background: white;
                padding: 1.5rem;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            }

            h1 { 
                font-size: 2rem;
                font-weight: 600;
                margin: 0;
            }

            .subtitle {
                color: #64748b;
                margin-top: 0.25rem;
            }

            /* Remove container padding */
            .container {
                flex: 1;
                display: flex;
                flex-direction: column;
            }

            /* Make map container fill available space */
            .map-container {
                flex: 1;
                display: flex;
                flex-direction: column;
                background: white;
            }

            /* Remove map borders and make it fill container */
            #map { 
                flex: 1;
                width: 100%;
                border: none;
                border-radius: 0;
            }

            /* Leaflet specific fixes */
            .leaflet-container {
                width: 100%;
                height: 100%;
                border-radius: 0;
            }
        """),Main(
            Div(
                P("A visualisation of elite runners' training metadata based on 2024 public Strava data (limited to the first 45 weeks, IAAF data pulled end Sept). Best viewed on a computer.", cls="subtitle"),
                P("Elite runners are arbitrarily given a cutoff of >= 1100 IAAF points.", cls="subtitle"),
                P(
                    "View the source code on ", 
                    A("GitHub", 
                      href="https://github.com/danielcwq/elite-endurance-viz/", 
                      target="_blank"
                    ),
                    cls="subtitle"
                ),
                P(
                    "Some athletes' training data might be inaccurate due to exact name matches on Strava accounts, am working on that!", cls="subtitle"
                ),
                cls="header"
            ),
            Div(
                Div(id="map"),
                cls="map-container"
            ),
            cls="container"
        ),
        Script(create_map_script(athletes_data))
    )

@rt("/athlete/{name}")
def get_athlete(name: str):
    # URL decode the name parameter
    decoded_name = unquote(name)
    
    # === CHANGED: Get athlete data from MongoDB ===
    athlete = db.get_athlete_metadata(decoded_name)
    if not athlete:
        return "Athlete not found", 404
    
    # === CHANGED: Get activities from MongoDB ===
    athlete_activities = db.get_athlete_activities(decoded_name)
    
    # Get athlete's Strava ID from activities
    athlete_strava_id = athlete_activities[0].get('Athlete ID') if athlete_activities else None
    
    # Process marks and disciplines
    marks = athlete.get('Mark', '').split('|') if athlete.get('Mark') else []
    disciplines = athlete.get('Discipline', '').split('|') if athlete.get('Discipline') else []
    event_times = list(zip(disciplines, marks)) if marks and disciplines else []

    # Find athlete in data
    #data_source = CSVDataSource('cleaned_athlete_metadata.csv')
    #activities_source = IndividualActivitiesDataSource('indiv_activities_full.csv')
    
    #data_manager = DataManager(data_source)
    #athletes_data = data_manager.load_data()
    #activities_data = activities_source.load_data()
    
    #athlete = athletes_data[athletes_data['Competitor'] == decoded_name].iloc[0]
    #athlete_activities = activities_data[activities_data['Athlete Name'].str.lower() == decoded_name.lower()].sort_values('Start Date', ascending=False)

    #marks = athlete['Mark'].split('|') if pd.notna(athlete['Mark']) else []
    #disciplines = athlete['Discipline'].split('|') if pd.notna(athlete['Discipline']) else []
    
    # Create a list of event-time pairs
    #event_times = list(zip(disciplines, marks)) if marks and disciplines else []
    #athlete_strava_id = athlete_activities['Athlete ID'].iloc[0] if not athlete_activities.empty else None
    
    return Titled(f"{decoded_name} - Elite Runners Database",
        get_analytics_script(),
        Style("""
            .container {
                max-width: 1200px;
                margin: 0 auto;
                padding: 2rem;
            }
              
            .event-times {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: 1rem;
                margin: 1.5rem 0;
            }

            .event-card {
                background: #f8fafc;
                padding: 1rem;
                border-radius: 0.5rem;
                border: 1px solid #e2e8f0;
            }

            .event-name {
                font-weight: 600;
                color: #1e293b;
                margin-bottom: 0.5rem;
            }

            .event-time {
                color: #64748b;
                font-size: 0.9rem;
            }
              
            .athlete-stats {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 1rem;
                margin: 2rem 0;
            }
            
            .stat-card {
                background: white;
                padding: 1.5rem;
                border-radius: 0.5rem;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            }
            
            .stat-value {
                font-size: 1.5rem;
                font-weight: 600;
                color: #1e293b;
            }
            
            .stat-label {
                font-size: 0.875rem;
                color: #64748b;
                margin-top: 0.5rem;
            }
            
            .activities-table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 2rem;
            }
            
            .activities-table th,
            .activities-table td {
                padding: 0.75rem;
                text-align: left;
                border-bottom: 1px solid #e2e8f0;
            }
            
            .activities-table th {
                background: #f8fafc;
                font-weight: 600;
            }
            
            .back-link {
                display: inline-block;
                margin-bottom: 2rem;
                color: #3b82f6;
                text-decoration: none;
            }
            
            .back-link:hover {
                text-decoration: underline;
            }

            .activity-cell {
                position: relative;
            }

            .activity-name {
                display: flex;
                align-items: center;
                gap: 0.5rem;
            }

            .dropdown-trigger {
                cursor: pointer;
                padding: 2px 5px;
                border-radius: 3px;
                background: #f0f0f0;
                font-size: 12px;
            }

            .description-box {
                display: none;
                position: absolute;
                left: 0;
                top: 100%;
                background: white;
                padding: 1rem;
                border-radius: 0.5rem;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                z-index: 1000;
                min-width: 200px;
                max-width: 400px;
                white-space: pre-wrap;
                margin-top: 0.5rem;
                border: 1px solid #e2e8f0;
            }

            .show-description {
                display: block !important;
            }
             .strava-link {
                color: inherit;
                text-decoration: none;
            }

            .strava-link:hover {
                color: #FC4C02;  /* Strava orange */
                text-decoration: underline;
            }

            .athlete-header {
                display: flex;
                align-items: center;
                gap: 0.5rem;
            }

            .strava-icon {
                width: 24px;
                height: 24px;
                vertical-align: middle;
            }
        """),
        Script("""
            function toggleDescription(event, id) {
                event.stopPropagation();
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
                P(f"Nationality: {athlete['Nat']}"),
                H2("Seasons Bests"),
                Div(
                    *[Div(
                        Div(event, cls="event-name"),
                        Div(time, cls="event-time"),
                        cls="event-card"
                    ) for event, time in event_times],
                    cls="event-times"
                ),
                H2("Training Stats"),
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
                    cls="athlete-stats"
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
                        Td(row.get('Start Date')),
                        Td(
                            Div(
                                Div(
                                    A(
                                        row.get('Activity Name'),
                                        href=f"https://strava.com/activities/{row.get('Activity ID')}" if row.get('Activity ID') else None,
                                        cls="strava-link",
                                        target="_blank"
                                    ) if row.get('Activity ID') else row.get('Activity Name'),
                                    Span("🔽", cls="dropdown-trigger", onclick=f"toggleDescription(event, {i})")
                                    if row.get('Description') else "",
                                    cls="activity-name"
                                ),
                                Div(row.get('Description', ''), cls="description-box", id=f"desc-{i}")
                                if row.get('Description') else "",
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