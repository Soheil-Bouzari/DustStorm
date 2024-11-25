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

# Function to add modular regex patterns
def add_patterns(matcher):
    # Define and add patterns for highways, interstates, distances, directions, and "near" phrases
    highway_patterns = [
        [{"LOWER": {"REGEX": r"(interstate|highway|route)\s*\d+"}}]  # Highways, interstates, routes
    ]

    distance_patterns = [
        [{"IS_DIGIT": True}, {"LOWER": {"IN": ["miles"]}}, {"LOWER": {"IN": ["north", "south", "east", "west"]}},
         {"LOWER": "of", "OP": "?"}, {"ENT_TYPE": "GPE"}]  # Distance + direction + place
    ]

    near_patterns = [
        [{"LOWER": "near"}, {"ENT_TYPE": "GPE"}]  # "near" followed by place
    ]

    # New Directional Distance Patterns: "X miles southwest of Y"
    directional_patterns = [
        [{"IS_DIGIT": True}, {"LOWER": {"IN": ["miles"]}}, {"LOWER": {"IN": ["southwest", "southeast", "northwest", "northeast"]}}, {"LOWER": "of"}, {"ENT_TYPE": "GPE"}]
    ]

    # Add modular patterns to matcher
    matcher.add("HighwayPatterns", highway_patterns)
    matcher.add("DistancePatterns", distance_patterns)
    matcher.add("NearPatterns", near_patterns)
    matcher.add("DirectionalDistancePatterns", directional_patterns)  # Add new directional patterns

# Call function to add patterns to matcher
add_patterns(matcher)

# Function to parse location information and create coherent phrases
def parse_location(narrative, state):
    doc = nlp(narrative)
    matches = matcher(doc)

    # Extract entities
    locations = []
    detailed_location = []

    # Log all detected entities (Debugging and Logging)
    print(f"---Processing Narrative---\n{narrative}")

    for ent in doc.ents:
        if ent.label_ == "GPE":  # Geopolitical entities (cities, states)
            locations.append(ent.text)
            print(f"Detected GPE: {ent.text}")  # Logging detected location

    # Add regex-based highway, distance, and directional distance matches
    for match_id, start, end in matches:
        span = doc[start:end].text
        detailed_location.append(span)
        # Log which pattern was matched (Debugging and Logging)
        match_rule = matcher.vocab.strings[match_id]
        print(f"Matched pattern: {match_rule} -> {span}")

    # Filter out unwanted terms like "zero" after matching
    filtered_locations = [loc for loc in detailed_location + locations if "zero" not in loc.lower()]

    # Prioritize distance-based patterns over plain place names
    prioritized_locations = []
    distance_based = any(re.search(r"\d+\s+miles\s+\w+", loc.lower()) for loc in filtered_locations)
    if distance_based:
        # Keep only distance-based location if found
        prioritized_locations = [loc for loc in filtered_locations if re.search(r"\d+\s+miles\s+\w+", loc.lower())]
    else:
        # Otherwise, add all valid filtered locations
        prioritized_locations = filtered_locations

    # Remove redundancy like plain city names if more detailed phrases exist
    final_locations = []
    for loc in prioritized_locations:
        if not any(re.search(r"\b" + re.escape(loc) + r"\b", other) and loc != other for other in prioritized_locations):
            final_locations.append(loc)

    # Create coherent location descriptions
    if final_locations:
        location_string = ", ".join(final_locations) + f", {state}"  # Include the state
        print(f"Final extracted location: {location_string}")  # Debugging
        return location_string

    print("No specific location found")
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
output_file = "combined_rows_final_output-9-30-2024.csv"
df_input.to_csv(output_file, index=False)

print(f"Location parsing complete. Results saved to {output_file}.")
