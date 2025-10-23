import os
import sys
import json
import urllib3
import certifi
import requests
from time import sleep

# Ensure correct number of arguments
if len(sys.argv) != 9:  # Remove the location argument
    print("Usage: python3 Subsetting_MERRA-2_Data_Argument.py <minlon> <maxlon> <minlat> <maxlat> <begTime> <endTime> <begHour> <endHour>")
    sys.exit(1)

# Read values from command-line arguments
minlon, maxlon, minlat, maxlat = map(float, sys.argv[1:5])
begTime, endTime, begHour, endHour = sys.argv[5:9]  # No location argument

# Set up HTTP request
http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
url = 'https://disc.gsfc.nasa.gov/service/subset/jsonwsp'

# Function to send requests
def get_http_data(request):
    hdrs = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    data = json.dumps(request)
    r = http.request('POST', url, body=data, headers=hdrs)
    response = json.loads(r.data)
    if response['type'] == 'jsonwsp/fault':
        print(f"API Error: {response['methodname']}")
        sys.exit(1)
    return response

# Request data from the API
subset_request = {
    'methodname': 'subset',
    'type': 'jsonwsp/request',
    'version': '1.0',
    'args': {
        'role': 'subset',
        'start': begTime,
        'end': endTime,
        'diurnalFrom': begHour,
        'diurnalTo': endHour,
        "diurnalAggregation": "none",
        'box': [minlon, minlat, maxlon, maxlat],
        'crop': True,
        'mapping': 'remapbil',
        'grid': 'cfsr0.5a',
        'data': [{'datasetId': 'M2T1NXFLX_V5.12.4', 'variable': v} for v in ['SPEED', 'SPEEDMAX', 'TLML', 'QLML', 'QSH', 'HLML']]
    }
}

response = get_http_data(subset_request)
job_id = response['result']['jobId']
print(f"Job ID: {job_id}")

# Check job status
status_request = {'methodname': 'GetStatus', 'version': '1.0', 'type': 'jsonwsp/request', 'args': {'jobId': job_id}}
while response['result']['Status'] in ['Accepted', 'Running']:
    sleep(5)
    response = get_http_data(status_request)
    print(f"Job status: {response['result']['Status']} ({response['result']['PercentCompleted']}% complete)")

if response['result']['Status'] != 'Succeeded':
    print(f"Job Failed: {response['fault']['code']}")
    sys.exit(1)

# Retrieve results
results_request = {'methodname': 'GetResult', 'version': '1.0', 'type': 'jsonwsp/request', 'args': {'jobId': job_id, 'count': 20, 'startIndex': 0}}
response = get_http_data(results_request)
results = response['result']['items']

# Process and download results
for item in results:
    if 'link' in item:
        url = item['link']
        result = requests.get(url)
        try:
            result.raise_for_status()
            filename = item['label']
            with open(filename, 'wb') as f:
                f.write(result.content)
            print(f"✅ {filename} is downloaded")
        except requests.exceptions.HTTPError as err:
            print(f"❌ Error: {err}")
            continue

print("✅ Downloading is done. Files are in your current working directory.")
