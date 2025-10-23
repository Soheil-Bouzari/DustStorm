import pandas as pd
import subprocess
import time
import os
import glob
import re
import logging

# Configure logging to write both to console and Log.txt
log_filename = "Log.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename, mode="w"),  # Overwrites Log.txt each run
        logging.StreamHandler()  # Also prints to console
    ]
)

# Define input Excel file
input_file = "Arizona_1996-2023_Final-12_19_2024_with_bounds_and_date_time.xlsx"

# Read Excel file
df = pd.read_excel(input_file)

# Loop through each row in the DataFrame
for index, row in df.iterrows():
    logging.info(f"Processing row {index + 1}/{len(df)}...")

    # Extract necessary parameters
    minlon = row["minlon"]
    maxlon = row["maxlon"]
    minlat = row["minlat"]
    maxlat = row["maxlat"]
    begTime = row["begTime"]
    endTime = row["endTime"]
    begHour = row["begHour"]
    endHour = begHour  # endHour is identical to begHour

    # Extract location name from Column T
    location_full = row["Extracted_Location - 9-29-2024"]
    location_name = location_full.split(",")[0].strip()  # Extract only the city name

    # Get the list of .nc files before running the script (to detect new downloads)
    existing_files = set(glob.glob("MERRA2_*.tavg1_2d_flx_Nx.*.SUB.nc"))

    # Call the Subsetting_MERRA-2_Data_Argument.py script
    try:
        subprocess.run([
            "python3", "Subsetting_MERRA-2_Data_Argument.py",
            str(minlon), str(maxlon), str(minlat), str(maxlat),
            begTime, endTime, begHour, endHour
        ], check=True)

        logging.info("✅ Downloading is done. Files are in your current working directory.")

        # Find the newly downloaded .nc file
        time.sleep(2)  # Allow time for the file to appear
        new_files = set(glob.glob("MERRA2_*.tavg1_2d_flx_Nx.*.SUB.nc")) - existing_files

        if new_files:
            for file in new_files:
                # Extract date using regex (YYYYMMDD pattern)
                match = re.search(r"\d{8}", file)
                if match:
                    date_part = match.group()
                else:
                    logging.warning(f"⚠️ Unexpected filename format: {file}")
                    date_part = "UNKNOWN_DATE"

                # Extract the MERRA2_XXX part dynamically
                prefix = file.split(".")[0]  # This will be "MERRA2_200", "MERRA2_300", etc.

                # New filename format
                new_filename = f"{prefix}.{date_part}.SUB.{location_name}.nc"

                # Rename the file
                os.rename(file, new_filename)
                logging.info(f"🔄 Renamed to: {new_filename}")
        else:
            logging.warning("⚠️ No new .nc file detected for renaming.")

        logging.info(f"✅ Row {index + 1} completed. Waiting 3 seconds before next request...\n")
        time.sleep(3)  # 3-second pause to prevent API throttling

    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Error processing row {index + 1}: {e}")
        logging.info("Skipping to the next row...\n")