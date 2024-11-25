import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Load your data from Excel
input_file = "combined_rows_final_output - 10-1-2024.xlsx"
df = pd.read_excel(input_file)

# Initialize geolocator
geolocator = Nominatim(user_agent="geo_app")

# Function to get coordinates
def get_coordinates(location_name):
    try:
        location = geolocator.geocode(location_name)
        if location:
            return f"{location.latitude}, {location.longitude}"
        else:
            return "Not Found"
    except GeocoderTimedOut:
        return "Timeout"

# Apply the function to the AZ column (replace 'AZ' with the actual column name)
df['Geocoordinate'] = df['Extracted_Location - 9-29-2024'].apply(get_coordinates)  # Update 'AZ' with the correct column name after inspecting the file

# Save the output to a new Excel file
output_file = "combined_rows_final_output_GeoLocation_11-17-2024.xlsx"
df.to_excel(output_file, index=False)
print(f"Geocoding complete. Results saved to {output_file}.")
