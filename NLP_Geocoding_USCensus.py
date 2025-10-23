import pandas as pd
import spacy
import requests

# Load the spaCy model for Named Entity Recognition
nlp = spacy.load("en_core_web_sm")

# US Census Geocoder API URL
us_census_geocode_url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"

# Load your data from CSV
input_file = "combined_rows.csv"
df_input = pd.read_csv(input_file)

# Function to extract location entities from text using spaCy
def extract_locations(text):
    doc = nlp(str(text))
    locations = [ent.text for ent in doc.ents if ent.label_ == "GPE"]  # GPE is for geopolitical entities
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

# Geocoding function with state restriction
def geocode_location(state, location):
    lat, lon, source = geocode_us_census(state, location)
    return lat, lon, source

# Initialize results list
results = []

# Process each row in the DataFrame
for index, row in df_input.iterrows():
    state = row['STATE']
    episode_narrative = row['EPISODE_NARRATIVE']
    event_narrative = row['EVENT_NARRATIVE']
    cz_name = row['CZ_NAME']
    
    # Extract and prioritize narratives for location extraction
    narratives = [episode_narrative, event_narrative]
    
    # Case 1: Geocode using narratives (if present)
    found_location = False
    for narrative in narratives:
        if pd.notnull(narrative):  # Check if narrative exists
            locations = extract_locations(narrative)
            for location in locations:
                lat, lon, source = geocode_location(state, location)
                if lat and lon:  # If valid coordinates found
                    df_input.at[index, 'BA'] = location  # Store location name
                    df_input.at[index, 'BB'] = lat  # Store latitude
                    df_input.at[index, 'BC'] = lon  # Store longitude
                    df_input.at[index, 'BD'] = f"Narrative: {location}"  # The narrative where the location was found
                    df_input.at[index, 'BE'] = source  # Indicate which geocoder was used
                    found_location = True
                    break  # Stop after finding first valid location
        if found_location:
            break

    # Case 2: Fallback to CZ_NAME if no valid narrative-based location is found
    if not found_location:
        if pd.notnull(cz_name):  # Use CZ_NAME as fallback if no location was found in the narrative
            locations = extract_locations(cz_name)  # Apply spaCy to CZ_NAME
            for location in locations:
                lat, lon, source = geocode_location(state, location)
                if lat and lon:
                    df_input.at[index, 'BA'] = location
                    df_input.at[index, 'BB'] = lat
                    df_input.at[index, 'BC'] = lon
                    df_input.at[index, 'BD'] = "No narrative location found, using CZ_NAME for location"
                    df_input.at[index, 'BE'] = source
                    break

# Save the updated DataFrame to a new CSV file
output_file = "combined_rows_USCensus.csv"
df_input.to_csv(output_file, index=False)

print(f"Geocoding complete. Results saved to {output_file}.")
