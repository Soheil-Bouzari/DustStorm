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

# Function to parse specific location information
def parse_location(narrative):
    doc = nlp(narrative)
    matches = matcher(doc)
    
    locations = []  # To store potential location matches
    for ent in doc.ents:
        if ent.label_ == "GPE":  # GPE = Geopolitical Entity (cities, states)
            locations.append(ent.text)
    
    # Handle highway/interstate parsing
    for match_id, start, end in matches:
        span = doc[start:end]
        locations.append(span.text)
    
    return locations

# Process each narrative and extract detailed locations
for index, row in df_input.iterrows():
    episode_narrative = row['EPISODE_NARRATIVE']
    event_narrative = row['EVENT_NARRATIVE']
    
    # Combine narratives into one text for processing
    full_narrative = f"{episode_narrative} {event_narrative}"
    
    # Extract specific locations
    parsed_locations = parse_location(full_narrative)
    
    if parsed_locations:
        # Store extracted locations in new columns for further geocoding
        df_input.at[index, 'Extracted_Location'] = ", ".join(parsed_locations)  # Store all found locations
        df_input.at[index, 'Location_Type'] = "Detailed Narrative Location"  # You can categorize based on keyword matches
    else:
        df_input.at[index, 'Extracted_Location'] = "No specific location found"
        df_input.at[index, 'Location_Type'] = "None"

# Save the output with new columns for parsed location details
output_file = "spaCy_improve.csv"
df_input.to_csv(output_file, index=False)

print(f"Location parsing complete. Results saved to {output_file}.")
