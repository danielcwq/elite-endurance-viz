from fasthtml.common import *
import json
import pandas as pd
from typing import Optional, Dict
from abc import ABC, abstractmethod

# Data Management
class DataSource(ABC):
    @abstractmethod
    def load_data(self) -> pd.DataFrame:
        pass

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
        'DZA': {'lat': 36.7538, 'lng': 3.0588},   # Algeria
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
        'DNK': {'lat': 56.2639, 'lng': 9.5018},   # Denmark
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
        'DEU': {'lat': 51.1657, 'lng': 10.4515},  # Germany
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
        'NLD': {'lat': 52.1326, 'lng': 5.2913},   # Netherlands
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
        'PRT': {'lat': 39.3999, 'lng': -8.2245},  # Portugal
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
        'ZAF': {'lat': -30.5595, 'lng': 22.9375}, # South Africa
        'KOR': {'lat': 35.9078, 'lng': 127.7669}, # South Korea
        'SSD': {'lat': 6.8770, 'lng': 31.3070},   # South Sudan
        'ESP': {'lat': 40.4637, 'lng': -3.7492},  # Spain
        'LKA': {'lat': 7.8731, 'lng': 80.7718},   # Sri Lanka
        'SDN': {'lat': 12.8628, 'lng': 30.2176},  # Sudan
        'SWE': {'lat': 60.1282, 'lng': 18.6435},  # Sweden
        'CHE': {'lat': 46.8182, 'lng': 8.2275},   # Switzerland
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
        'URY': {'lat': -32.5228, 'lng': -55.7658}, # Uruguay
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
    hdrs=(
        Link(rel='stylesheet', href='https://unpkg.com/leaflet@1.7.1/dist/leaflet.css'),
        Link(rel='stylesheet', href='https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.css'),
        Link(rel='stylesheet', href='https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.Default.css'),
        Script(src='https://unpkg.com/leaflet@1.7.1/dist/leaflet.js'),
        Script(src='https://unpkg.com/leaflet.markercluster@1.4.1/dist/leaflet.markercluster.js')
    )
)

def create_map_script(athletes_data: list) -> str:
    return f"""
        var map = L.map('map').setView([0, 0], 2);
        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '© OpenStreetMap contributors'
        }}).addTo(map);

        var athletes = {json.dumps(athletes_data)};
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
                    return '<li>' + a['Athlete Name'] + ' (' + a.Discipline + ')</li>';
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

@rt("/")
def get():
    # Initialize data
    data_source = CSVDataSource('cleaned_athlete_metadata.csv')
    data_manager = DataManager(data_source)
    athletes_data = data_manager.load_data().to_dict('records')
    
    return Titled("Elite Athletes Map",
        Style("""
            body { margin: 0; padding: 0; font-family: Arial, sans-serif; }
            .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
            h1 { text-align: center; color: #333; margin-bottom: 30px; }
            #map { height: 600px; width: 100%; }
        """),
        Main(
            Div(
                H1("Elite Endurance Athletes Worldwide"),
                Div(id="map"),
                cls="container"
            )
        ),
        Script(create_map_script(athletes_data))
    )

if __name__ == "__main__":
    serve(host='0.0.0.0', port=8000)