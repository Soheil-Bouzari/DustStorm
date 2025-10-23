import re
import pandas as pd
import spacy
from spacy.matcher import Matcher

# Load the spaCy model for Named Entity Recognition
nlp = spacy.load("en_core_web_sm")

# Load your data from CSV
input_file = "combined_rows.csv"
df_input = pd.read_csv(input_file)

# Initialize the Matcher
matcher = Matcher(nlp.vocab)

# Define patterns for highways, interstates, distances, directions, and "near" phrases
patterns = [
    [{"LOWER": {"REGEX": r"(interstate|highway|route)\s*\d+"}}],  # Interstates and highways
    [{"IS_DIGIT": True}, {"LOWER": {"IN": ["miles"]}}, {"LOWER": {"IN": ["north", "south", "east", "west"]}}, {"LOWER": "of", "OP": "?"}, {"ENT_TYPE": "GPE"}],  # Distance + direction + optional "of" + place
    [{"LOWER": "near"}, {"ENT_TYPE": "GPE"}],  # "near" followed by a place
]

matcher.add("LocationPatterns", patterns)

# Function to parse location information and create coherent phrases
def parse_location(narrative, state):
    doc = nlp(narrative)
    matches = matcher(doc)
    
    # Extract entities
    locations = []
    detailed_location = []
    
    for ent in doc.ents:
        if ent.label_ == "GPE":  # Geopolitical entities (cities, states)
            locations.append(ent.text)
    
    # Add regex-based highway and distance matches
    for match_id, start, end in matches:
        span = doc[start:end].text
        detailed_location.append(span)
    
    # Filter out unwanted terms like "zero" after matching
    filtered_locations = [loc for loc in detailed_location + locations if "zero" not in loc.lower()]
    
    # Remove redundancy like plain city names if more detailed phrases exist
    final_locations = []
    for loc in filtered_locations:
        if not any(re.search(r"\b" + re.escape(loc) + r"\b", other) and loc != other for other in filtered_locations):
            final_locations.append(loc)
    
    # Create coherent location descriptions
    if final_locations:
        location_string = ", ".join(final_locations) + f", {state}"  # Include the state
        return location_string
    
    return "No specific location found"

# Process each narrative and extract detailed locations
for index, row in df_input.iterrows():
    episode_narrative = row['EPISODE_NARRATIVE']
    event_narrative = row['EVENT_NARRATIVE']
    state = row['STATE']
    
    # Combine narratives into one text for processing
    full_narrative = f"{episode_narrative} {event_narrative}"
    
    # Extract and format specific locations
    parsed_location = parse_location(full_narrative, state)
    
    # Store results in new columns
    df_input.at[index, 'Extracted_Location'] = parsed_location

# Save the output with the extracted locations
output_file = "combined_rows_final_output.csv"
df_input.to_csv(output_file, index=False)

print(f"Location parsing complete. Results saved to {output_file}.")
