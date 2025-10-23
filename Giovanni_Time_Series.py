# Import Libraries
import netrc
import requests
from requests.auth import HTTPBasicAuth
import os
import io
import pandas as pd  # Renamed for consistency
import numpy as np
import matplotlib.pyplot as plt

# Setup the signin and time series URLs
signin_url = "https://api.giovanni.earthdata.nasa.gov/signin"
time_series_url = "https://api.giovanni.earthdata.nasa.gov/timeseries"

# Store Parameters
lat = 45.125
lon = -98.125
time_start = "2000-01-01T03:00:00"
time_end = "2020-09-30T21:00:00"
data = "GLDAS_NOAH025_3H_2_1_Tair_f_inst"

# Retrieve Token
try:
    earthdata_credentials = netrc.netrc().hosts['urs.earthdata.nasa.gov']
    token = requests.get(
        signin_url,
        auth=HTTPBasicAuth(earthdata_credentials[0], earthdata_credentials[2]),
        allow_redirects=True
    ).text.replace('"', '')
except Exception as e:
    print("Error retrieving token:", e)
    exit()

# Define a Function That Calls the Time Series Service
def call_time_series(lat, lon, time_start, time_end, data):
    """
    INPUTS:
    lat - latitude
    lon - longitude
    time_start - start of time series in YYYY-MM-DDThh:mm:ss format (UTC)
    end_time - end of the time series in YYYY-MM-DDThh:mm:ss format (UTC)
    data - name of the data parameter for the time series
    
    OUTPUT:
    time series csv output string
    """
    query_parameters = {
        "data": data,
        "location": "[{},{}]".format(lat, lon),
        "time": "{}/{}".format(time_start, time_end)
    }
    headers = {"authorizationtoken": token}
    response = requests.get(time_series_url, params=query_parameters, headers=headers)
    return response.text

# Define a Function That Parses the Response From the Time Series Service
def parse_csv(ts):
    """
    INPUTS:
    ts - time series output of the time series service
    
    OUTPUTS:
    headers,df - the headers from the CSV as a dict and the values in a pandas dataframe
    """
    with io.StringIO(ts) as f:
        # the first 13 rows are header
        headers = {}
        for i in range(13):
            line = f.readline()
            key, value = line.split(",")
            headers[key] = value.strip()

        # Read the csv proper
        df = pd.read_csv(
            f,
            header=1,
            names=("Timestamp", headers["param_name"]),
            converters={"Timestamp": pd.Timestamp}
        )

    return headers, df

# Call the Time Series Service
print("Fetching time series data...")
ts = call_time_series(lat, lon, time_start, time_end, data)

# Parse the output
headers, df = parse_csv(ts)
print(df.head())

# Plot Results
ax = plt.gca()
df.plot(x="Timestamp", y=headers["param_name"], ax=ax)
ax.set_ylabel(headers["unit"])
ax.set_title("{} at [{},{}]".format(data, headers["lat"], headers["lon"]))

# Save the plot (optional)
plt.savefig("time_series_plot.png")
plt.show()
