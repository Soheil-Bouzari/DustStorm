import os
import re

# Define the directory containing the .nc files
directory = "/mnt/c/DevNet/NASA/NC Files/Renamed NC Files"  # Change this to the actual path

# Regular expression pattern to match the filenames
pattern = re.compile(r"MERRA2_\d{3}\.(\d{8})\.SUB\.(.+)\.nc")

# Iterate through the files in the directory
for filename in os.listdir(directory):
    match = pattern.match(filename)
    if match:
        date_part = match.group(1)  # Extract the date part (YYYYMMDD)
        city_name = match.group(2)  # Extract the city name

        # Remove spaces, hyphens, and apostrophes from the city name
        city_name = re.sub(r"[\s\-\']", "", city_name)

        # Construct the new filename
        new_filename = f"MERRA{date_part}{city_name}.nc"

        # Get full file paths
        old_file_path = os.path.join(directory, filename)
        new_file_path = os.path.join(directory, new_filename)

        # Rename the file
        os.rename(old_file_path, new_file_path)
        print(f"Renamed: {filename} -> {new_filename}")

print("Renaming completed.")
