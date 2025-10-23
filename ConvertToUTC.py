import pandas as pd
from datetime import datetime, timedelta

# Load the Excel file
input_file = "Arizona_1996-2023_Final-12_19_2024_with_bounds.xlsx"
df = pd.read_excel(input_file)

# Function to convert time
def convert_to_utc(yearmonth, day, time):
    # Ensure BEGIN_TIME is a valid 4-digit string
    time_str = str(time).zfill(4)
    hour = int(time_str[:2])
    minute = int(time_str[2:])

    # Construct datetime object in Arizona time
    date_str = f"{str(yearmonth)[:4]}-{str(yearmonth)[4:6]}-{day}"
    az_time = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(hours=hour, minutes=minute)

    # Convert Arizona time to UTC (add 7 hours)
    utc_time = az_time + timedelta(hours=7)

    # Round to the nearest 30 minutes
    if utc_time.minute == 0:
        utc_time = utc_time.replace(minute=30)
    elif 1 <= utc_time.minute <= 29:
        utc_time = utc_time.replace(minute=30)
    elif utc_time.minute >= 31:
        utc_time = utc_time.replace(minute=30) + timedelta(hours=1)

    # Format the date correctly
    beg_time = utc_time.strftime("%Y-%m-%dT00:00:00Z")
    end_time = utc_time.strftime("%Y-%m-%dT23:00:00Z")
    beg_hour = utc_time.strftime("%H:%M")

    return beg_time, end_time, beg_hour

# Apply the function
df[['begTime', 'endTime', 'begHour']] = df.apply(
    lambda row: convert_to_utc(row['BEGIN_YEARMONTH'], row['BEGIN_DAY'], row['BEGIN_TIME']),
    axis=1, result_type='expand'
)

# Save to a new file
output_file = "Arizona_1996-2023_Final-12_19_2024_with_bounds_and_date_time.xlsx"
df.to_excel(output_file, index=False)

print(f"Conversion complete. Saved to {output_file}.")
