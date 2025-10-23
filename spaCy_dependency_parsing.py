import pandas as pd
import spacy
from spacy.matcher import PhraseMatcher

# Load the spaCy model for Named Entity Recognition
nlp = spacy.load("en_core_web_sm")

# Load your data from CSV
input_file = "combined_rows.csv"
df_input = pd.read_csv(input_file)

# Define patterns to detect highways, interstates, and directional keywords
highway_keywords = ["highway", "interstate", "hwy", "route"]
directional_keywords = ["north", "south", "east", "west", "miles"]

# Initialize PhraseMatcher for highways and keywords
matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
patterns = [nlp.make_doc(kw) for kw in highway_keywords + directional_keywords]
matcher.add("HighwayTerms", patterns)

# Function to combine distance, direction, and place into a cohesive location phrase
def combine_location(distance_phrase, direction, location, highway=None):
    if highway:
        return f"{distance_phrase} {direction} of {location}, on {highway}"
    else:
        return f"{distance_phrase} {direction} of {location}"

# Function to parse specific location information using dependency parsing
def parse_location(narrative):
    doc = nlp(narrative)
    matches = matcher(doc)

    locations = []  # To store potential location matches
    highway = None  # To store highway if detected
    
    # First, handle highway/interstate parsing
    for match_id, start, end in matches:
        span = doc[start:end]
        if any([kw in span.text.lower() for kw in highway_keywords]):
            highway = span.text  # Store the highway for future use
    
    # Use dependency parsing to find distance + direction + location relationships
    for token in doc:
        # Find numerical quantities followed by distance keywords and directions
        if token.like_num and token.head.text in ["miles", "kilometers"] and token.dep_ in ["nummod"]:
            # Look for direction and location attached to this token
            distance_phrase = f"{token.text} {token.head.text}"
            direction = ""
            location = ""

            # Find direction (e.g., "south") and location (e.g., "Phoenix")
            for child in token.head.children:
                if child.text.lower() in directional_keywords:
                    direction = child.text
                if child.ent_type_ == "GPE":  # Geopolitical entity (location)
                    location = child.text

            # Look for locations outside of the distance + direction dependency tree
            if not location:
                for child in token.children:
                    if child.ent_type_ == "GPE":  # Geopolitical entity (location)
                        location = child.text
            
            # Create a phrase combining distance, direction, and location
            if direction and location:
                full_location = combine_location(distance_phrase, direction, location, highway)
                locations.append(full_location)

    return locations

# Process each narrative and extract detailed locations
for index, row in df_input.iterrows():
    episode_narrative = row['EPISODE_NARRATIVE']
    event_narrative = row['EVENT_NARRATIVE']

    # Combine narratives into one text for processing
    full_narrative = f"{episode_narrative} {event_narrative}"

    # Extract specific locations using dependency parsing
    parsed_locations = parse_location(full_narrative)

    if parsed_locations:
        # Store extracted locations in new columns for further geocoding
        df_input.at[index, 'Extracted_Location'] = ", ".join(parsed_locations)  # Store all found locations
        df_input.at[index, 'Location_Type'] = "Detailed Narrative Location"  # You can categorize based on keyword matches
    else:
        df_input.at[index, 'Extracted_Location'] = "No specific location found"
        df_input.at[index, 'Location_Type'] = "None"

# Save the output with new columns for parsed location details
output_file = "spaCy_improve_with_better_dependencies.csv"
df_input.to_csv(output_file, index=False)

print(f"Location parsing complete. Results saved to {output_file}.")
