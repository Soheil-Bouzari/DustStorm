import pandas as pd
import spacy
import requests
from geopy.geocoders import GeoNames

# Load the spaCy model for Named Entity Recognition
nlp = spacy.load("en_core_web_sm")

# Initialize the GeoNames geocoder
geo_names_user = "soheil.bouzari"  # Replace with your GeoNames username
geonames = GeoNames(username=geo_names_user)

# US Census Geocoder API URL
us_census_geocode_url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"

# Load your data from CSV
input_file = "combined_rows.csv"
df_input = pd.read_csv(input_file)

# Function to extract location entities from text using spaCy
def extract_locations(text):
    doc = nlp(str(text))
    locations = [ent.text for ent in doc.ents if ent.label_ == "GPE"]
    return locations

# Function to geocode using US Census Geocoder API
def geocode_us_census(state, location):
    try:
        params = {
            'address': f'{location}, {state}',
            'benchmark': 'Public_AR_Current',
            'format': 'json'
        }
        response = requests.get(us_census_geocode_url, params=params)
        response_json = response.json()

        # Parse response to extract coordinates
        if response_json['result']['addressMatches']:
            coordinates = response_json['result']['addressMatches'][0]['coordinates']
            return coordinates['y'], coordinates['x'], "US Census Geocoder"  # Return latitude, longitude, and source
        return None, None, None
    except Exception as e:
        print(f"US Census Geocoder error: {e}")
        return None, None, None

# Function to geocode using GeoNames API
def geocode_geonames(state, location):
    try:
        geo = geonames.geocode(f"{location}, {state}")
        if geo:
            return geo.latitude, geo.longitude, "GeoNames"  # Return latitude, longitude, and source
        return None, None, None
    except Exception as e:
        print(f"GeoNames error: {e}")
        return None, None, None

# Geocoding function combining both geocoders
def geocode_location(state, location):
    lat, lon, source = geocode_us_census(state, location)
    if not lat or not lon:
        lat, lon, source = geocode_geonames(state, location)
    return lat, lon, source

# Initialize results list
results = []

# Process each row in the DataFrame
for index, row in df_input.iterrows():
    state = row['STATE']
    episode_narrative = row['EPISODE_NARRATIVE']
    event_narrative = row['EVENT_NARRATIVE']
    cz_name = row['CZ_NAME']
    
    # Extract locations from narratives
    locations = []
    for narrative in [episode_narrative, event_narrative]:
        if pd.notnull(narrative):
            extracted_locations = extract_locations(narrative)
            locations.extend(extracted_locations)
    
    # Case 1: Geocode using extracted locations from narratives
    found_location = False
    narrative_found = bool(locations)  # Flag to track if narrative contains locations
    for location in locations:
        lat, lon, source = geocode_location(state, location)
        if lat and lon:
            df_input.at[index, 'BA'] = location  # Store location name
            df_input.at[index, 'BB'] = lat  # Store latitude
            df_input.at[index, 'BC'] = lon  # Store longitude
            df_input.at[index, 'BD'] = f"Narrative: {location}"  # The narrative where the location was found
            df_input.at[index, 'BE'] = source  # Indicate which geocoder was used
            found_location = True
            break  # Stop after finding the first valid location
    
    # Case 2: Fallback to CZ_NAME if no valid location from narratives is found
    if not found_location:
        if narrative_found:
            df_input.at[index, 'BD'] = "Narrative found, but geocoding failed"
        else:
            if pd.notnull(cz_name):
                lat, lon, source = geocode_location(state, cz_name)
                df_input.at[index, 'BA'] = cz_name
                df_input.at[index, 'BB'] = lat
                df_input.at[index, 'BC'] = lon
                df_input.at[index, 'BD'] = "No narrative location found, using CZ_NAME for location"
                df_input.at[index, 'BE'] = source  # Indicate which geocoder was used

# Save the updated DataFrame to a new CSV file
output_file = "combined_rows_identify_which_geocode.csv"
df_input.to_csv(output_file, index=False)

print(f"Geocoding complete. Results saved to {output_file}.")
