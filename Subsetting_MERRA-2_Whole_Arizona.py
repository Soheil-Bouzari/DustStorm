import sys
import json
import urllib3
import certifi
import requests
import pandas as pd
from time import sleep

# Load the Excel file
input_file = "Arizona_1996-2023_Final-12_19_2024_with_bounds_and_date_time.xlsx"
df = pd.read_excel(input_file)

# Create a urllib PoolManager instance to make requests
http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

# Set the URL for the GES DISC subset service endpoint
url = "https://disc.gsfc.nasa.gov/service/subset/jsonwsp"

# Function to send a request with retry mechanism
def get_http_data(request, retries=3):
    hdrs = {"Content-Type": "application/json", "Accept": "application/json"}
    data = json.dumps(request)

    for attempt in range(retries):
        try:
            r = http.request("POST", url, body=data, headers=hdrs)
            if r.status != 200:
                print(f"Error: Received HTTP {r.status} on attempt {attempt + 1}")
                sleep(5)
                continue  # Retry if not a 200 response
            
            response = json.loads(r.data)
            if response.get("type") == "jsonwsp/fault":
                print(f"API Error: {response.get('methodname', 'Unknown')} request")
                sleep(5)
                continue  # Retry if a JSON error occurs
            return response  # Success

        except (json.decoder.JSONDecodeError, urllib3.exceptions.HTTPError) as e:
            print(f"Error: {str(e)}. Retrying {attempt + 1}/{retries}...")
            sleep(5)

    print("Failed after multiple retries.")
    sys.exit(1)  # Exit script if all retries fail

# Variables
product = "M2T1NXFLX_V5.12.4"
varNames = ["SPEED", "SPEEDMAX", "TLML", "QLML", "QSH", "HLML"]
interp = "remapbil"
destGrid = "cfsr0.5a"

# Iterate over each row
for index, row in df.iterrows():
    minlon = row["minlon"]
    maxlon = row["maxlon"]
    minlat = row["minlat"]
    maxlat = row["maxlat"]
    begTime = row["begTime"]
    endTime = row["endTime"]
    begHour = row["begHour"]
    endHour = begHour  # Same as begHour

    print(f"\nProcessing row {index + 1}/{len(df)}")

    # Construct JSON WSP request
    subset_request = {
        "methodname": "subset",
        "type": "jsonwsp/request",
        "version": "1.0",
        "args": {
            "role": "subset",
            "start": begTime,
            "end": endTime,
            "diurnalFrom": begHour,
            "diurnalTo": endHour,
            "diurnalAggregation": "none",
            "box": [minlon, minlat, maxlon, maxlat],
            "crop": True,
            "mapping": interp,
            "grid": destGrid,
            "data": [{"datasetId": product, "variable": var} for var in varNames],
        },
    }

    # Submit request with retry
    response = get_http_data(subset_request)
    if not response or "result" not in response:
        print(f"Row {index + 1} - Invalid response, skipping.")
        continue  # Skip this row if response is invalid

    myJobId = response["result"]["jobId"]
    print(f"Row {index + 1} - Job ID: {myJobId}")
    print(f"Job status: {response['result']['Status']}")

    # Monitor job status
    status_request = {
        "methodname": "GetStatus",
        "version": "1.0",
        "type": "jsonwsp/request",
        "args": {"jobId": myJobId},
    }

    wait_time = 0
    while response["result"]["Status"] in ["Accepted", "Running"]:
        sleep(5)
        wait_time += 5
        response = get_http_data(status_request)
        if not response or "result" not in response:
            print(f"Row {index + 1} - Error getting status. Skipping.")
            continue
        status = response["result"]["Status"]
        percent = response["result"]["PercentCompleted"]
        print(f"Row {index + 1} - Job status: {status} ({percent}% complete)")

        # Ensure at least 15 seconds per row processing
        if wait_time < 15:
            sleep(15 - wait_time)

    if response["result"]["Status"] != "Succeeded":
        print(f"Row {index + 1} - Job Failed")
        continue

    print(f"Row {index + 1} - Job Finished")

    # Retrieve results
    batchsize = 20
    results_request = {
        "methodname": "GetResult",
        "version": "1.0",
        "type": "jsonwsp/request",
        "args": {"jobId": myJobId, "count": batchsize, "startIndex": 0},
    }

    results = []
    response = get_http_data(results_request)
    if not response or "result" not in response:
        print(f"Row {index + 1} - Error retrieving results. Skipping.")
        continue

    count = response["result"]["itemsPerPage"]
    results.extend(response["result"]["items"])
    total = response["result"]["totalResults"]

    while count < total:
        results_request["args"]["startIndex"] += batchsize
        response = get_http_data(results_request)
        if not response or "result" not in response:
            print(f"Row {index + 1} - Error retrieving more results. Skipping.")
            continue
        count += response["result"]["itemsPerPage"]
        results.extend(response["result"]["items"])

    # Download the results
    download_success = False
    for item in results:
        if "link" in item:
            url = item["link"]
            result = requests.get(url)
            if result.status_code == 200:
                filename = item["label"]
                with open(filename, "wb") as f:
                    f.write(result.content)
                print(f"Row {index + 1} - Downloaded: {filename}")
                download_success = True
            else:
                print(f"Row {index + 1} - Error downloading: {url}")

    if download_success:
        print("Downloading is done and find the downloaded files in your current working directory")
    
    # Introduce a 10-15 second delay to avoid API throttling
    sleep(120 + (5 * (index % 2)))

print("All requests completed.")
