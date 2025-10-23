import pandas as pd
import spacy
from geopy.geocoders import Nominatim

# Load the spaCy model
nlp = spacy.load("en_core_web_sm")

# Initialize geocoder
geolocator = Nominatim(user_agent="dust_storm_geocoder")

# Load your data from CSV
input_file = "combined_rows.csv"  # Ensure this file is in the same directory as the script
df_input = pd.read_csv(input_file)

# Check if the columns 'BA', 'BB', 'BC', and 'BD' exist, and if not, create them
if 'BA' not in df_input.columns:
    df_input['BA'] = ''  # Empty string for location name

if 'BB' not in df_input.columns:
    df_input['BB'] = None  # None for latitude

if 'BC' not in df_input.columns:
    df_input['BC'] = None  # None for longitude

if 'BD' not in df_input.columns:
    df_input['BD'] = ''  # Empty string for narrative

# Function to extract location entities
def extract_locations(text):
    doc = nlp(str(text))
    locations = [ent.text for ent in doc.ents if ent.label_ == "GPE"]
    return locations

# Function to geocode locations
def geocode_location(location, state):
    try:
        geo = geolocator.geocode(f"{location}, {state}")
        if geo:
            return (geo.latitude, geo.longitude)
        else:
            return (None, None)
    except:
        return (None, None)

# Process each narrative in both columns and geocode locations
for index, row in df_input.iterrows():
    narratives = [row['EPISODE_NARRATIVE'], row['EVENT_NARRATIVE']]
    state = row['STATE']  # Use the state column to restrict geocoding to the correct state
    
    # To store results in BA, BB, BC, BD columns
    latitude, longitude, location = None, None, None
    
    for narrative in narratives:
        locations = extract_locations(narrative)
        if locations:
            for loc in locations:
                lat, lon = geocode_location(loc, state)
                if lat and lon:
                    location = loc
                    latitude, longitude = lat, lon
                    break  # Stop once we have a valid lat/lon

    # Store results in columns BA, BB, BC, BD
    df_input.at[index, 'BA'] = location  # Location name
    df_input.at[index, 'BB'] = latitude  # Latitude
    df_input.at[index, 'BC'] = longitude  # Longitude
    df_input.at[index, 'BD'] = narrative  # The narrative where the location was found

# Save updated data back to the same file
df_input.to_csv(input_file, index=False)
