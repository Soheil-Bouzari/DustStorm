import pandas as pd
import spacy
from spacy.matcher import PhraseMatcher
from geopy.geocoders import GeoNames

# Load the spaCy model for Named Entity Recognition
nlp = spacy.load("en_core_web_sm")

# Initialize the GeoNames geocoder
geo_names_user = "soheil.bouzari"  # Replace with your GeoNames username
geonames = GeoNames(username=geo_names_user)

# Load your data from CSV
input_file = "combined_rows.csv"
df_input = pd.read_csv(input_file)

# Create a PhraseMatcher to detect specific terms
matcher = PhraseMatcher(nlp.vocab, attr="LOWER")

# Add custom terms for locations and abbreviations
terms = [
    "National Recreation Area", "Highway 54 corridor", "Desert Park", "National Park",
    "Automated Surface Observing System", "Automated Weather Observing System", "NMDOT"
]
patterns = [nlp.make_doc(term) for term in terms]
matcher.add("LOCATION_TERMS", patterns)

# Abbreviation handling (converting abbreviations to their expanded form)
abbreviation_map = {
    "N.W.": "North West",
    "N.P.": "National Park",
    "ASOS": "Automated Surface Observing System",
    "AWOS": "Automated Weather Observing System",
    "NMDOT": "New Mexico Department of Transportation (Road Camera)"
}

# Define patterns to detect highways, interstates, and directional keywords
highway_keywords = ["highway", "interstate", "hwy", "route"]
directional_keywords = ["north", "south", "east", "west", "miles"]

# Function to normalize abbreviations in text
def normalize_abbreviations(text):
    for abbr, full_form in abbreviation_map.items():
        text = text.replace(abbr, full_form)
    return text

# Function to check for highways and directional keywords
def check_highways_and_directions(doc):
    highways = []
    directions = []
    for token in doc:
        if token.text.lower() in highway_keywords:
            highways.append(token.text)
        if token.text.lower() in directional_keywords:
            directions.append(token.text)
    return highways, directions

# Function to extract location entities from text using spaCy and PhraseMatcher
def extract_locations(text):
    text = normalize_abbreviations(text)  # Normalize abbreviations
    doc = nlp(text)
    locations = [ent.text for ent in doc.ents if ent.label_ == "GPE"]

    # Use PhraseMatcher to detect specific terms
    matches = matcher(doc)
    for match_id, start, end in matches:
        span = doc[start:end]
        locations.append(span.text)

    # Extract highways and directions
    highways, directions = check_highways_and_directions(doc)
    if highways:
        locations.extend(highways)
    if directions:
        locations.extend(directions)
    
    return locations

# Geocoding function using GeoNames API
def geocode_geonames(state, location):
    try:
        geo = geonames.geocode(f"{location}, {state}")
        if geo:
            return geo.latitude, geo.longitude, "GeoNames"
        return None, None, None
    except Exception as e:
        print(f"GeoNames error: {e}")
        return None, None, None

# Process each row in the DataFrame
for index, row in df_input.iterrows():
    state = row['STATE']
    episode_narrative = row['EPISODE_NARRATIVE']
    event_narrative = row['EVENT_NARRATIVE']
    
    # Extract locations from narratives
    locations = []
    for narrative in [episode_narrative, event_narrative]:
        if pd.notnull(narrative):
            extracted_locations = extract_locations(narrative)
            locations.extend(extracted_locations)
    
    # Geocode the extracted locations
    found_location = False
    for location in locations:
        lat, lon, source = geocode_geonames(state, location)
        if lat and lon:
            df_input.at[index, 'Extracted Location'] = location
            df_input.at[index, 'Latitude'] = lat
            df_input.at[index, 'Longitude'] = lon
            df_input.at[index, 'Geocoder'] = source
            found_location = True
            break  # Stop after finding the first valid location

    # Fallback if no valid location was found
    if not found_location:
        df_input.at[index, 'Extracted Location'] = "No valid location found"

# Save the updated DataFrame to a new CSV file
output_file = "combined_rows_with_highways_and_directions.csv"
df_input.to_csv(output_file, index=False)

print(f"Processing complete. Results saved to {output_file}.")
