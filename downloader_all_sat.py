import os
import sys
import json
import requests
import argparse
import datetime
import re
import pandas as pd

def sendRequest(url, data, apiKey=None):
    json_data = json.dumps(data)
    headers = {'Content-Type': 'application/json'}
    if apiKey:
        headers['X-Auth-Token'] = apiKey
    response = requests.post(url, data=json_data, headers=headers)
    response.raise_for_status()
    output = response.json()
    return output['data']

def downloadFile(url, path):
    try:
        response = requests.get(url, stream=True)
        disposition = response.headers.get('content-disposition', '')
        matches = re.findall(r"filename=(.+)", disposition)
        if not matches:
            print("Could not find filename in content-disposition header.")
            return
        filename = matches[0].strip("\"")
        print(f"Downloading {filename} ...\n")
        with open(os.path.join(path, filename), 'wb') as f:
            f.write(response.content)
        print(f"Downloaded {filename}\n")
    except Exception as e:
        print(f"Download Failed: {e}")

def get_valid_datasets(start_date, end_date):
    valid = set()
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    def overlaps(s1, e1, s2, e2):
        return max(s1, s2) <= min(e1, e2)

    # Landsat 4 & 5: TM → 1982–2011
    if overlaps(start, end, datetime.date(1982, 7, 16), datetime.date(2011, 6, 5)):
        valid.add("landsat_tm_c2_l2")

    # Landsat 7: ETM+ → 1999–2022
    if overlaps(start, end, datetime.date(1999, 4, 15), datetime.date(2022, 4, 6)):
        valid.add("landsat_etm_c2_l2")

    # Landsat 8 & 9: OLI/TIRS → dal 2013 in poi
    if overlaps(start, end, datetime.date(2013, 2, 11), datetime.date.today()):
        valid.add("landsat_ot_c2_l2")

    return list(valid)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', required=True, help='ERS Username')
    parser.add_argument('--token', required=True, help='ERS application token')
    parser.add_argument('--bbox', type=float, nargs=4, required=True, 
                        help='Bounding box as xmin ymin xmax ymax (lon_min lat_min lon_max lat_max)')
    parser.add_argument('--start_date', type=str, required=True, help='Start date (yyyy-mm-dd)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (yyyy-mm-dd)')
    parser.add_argument('--city', type=str, default='Bologna', help='City name (not used for folders here).')
    parser.add_argument('--out_dir', type=str, default='.', 
                        help='Directory where the downloaded images will be saved. Default is current directory.')
    args = parser.parse_args()

    username = args.username
    token = args.token
    xmin, ymin, xmax, ymax = args.bbox
    start = args.start_date
    end = args.end_date
    out_dir = args.out_dir

    # Selezione dinamica dei dataset
    datasetNames = get_valid_datasets(start, end)
    print(f"Using datasets: {datasetNames}")

    bandNames = ['QA_PIXEL', 'ST_B10', 'ST_B6']
    serviceUrl = "https://m2m.cr.usgs.gov/api/api/json/stable/"

    # Login
    payload = {'username': username, 'token': token}
    apiKey = sendRequest(serviceUrl + "login-token", payload)

    all_downloads = []

    for datasetName in datasetNames:
        print(f"\nSearching dataset: {datasetName}")
        payload = {
            'datasetName': datasetName,
            'sceneFilter': {
                'acquisitionFilter': {'start': start, 'end': end},
                'spatialFilter': {
                    'filterType': 'mbr',
                    'lowerLeft':  {'latitude': ymin, 'longitude': xmin},
                    'upperRight': {'latitude': ymax, 'longitude': xmax}
                },
                'cloudCoverFilter': {'max': 70}
            }
        }

        scenes = sendRequest(serviceUrl + "scene-search", payload, apiKey)

        if not scenes or 'results' not in scenes or len(scenes['results']) == 0:
            print(f"No scenes found in {datasetName}")
            continue

        sceneIds = [scene['entityId'] for scene in scenes['results']]

        # Download options
        payload = {
            'datasetName': datasetName,
            'entityIds': sceneIds,
            'includeSecondaryFileGroups': True
        }

        try:
            options = sendRequest(serviceUrl + "download-options", payload, apiKey)
        except Exception as e:
            print(f"Error fetching download options for {datasetName}: {e}")
            continue

        options = pd.json_normalize(options)
        for _, option in options.iterrows():
            if option.get('secondaryDownloads'):
                for item in option['secondaryDownloads']:
                    for bandName in bandNames:
                        if item["bulkAvailable"] and bandName in item['displayId']:
                            all_downloads.append({
                                "entityId": item["entityId"],
                                "productId": item["id"]
                            })

    if not all_downloads:
        print("No valid downloads found.")
        sys.exit(0)

    # Submit download request
    label = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = {'downloads': all_downloads, 'label': label}
    requestResults = sendRequest(serviceUrl + "download-request", payload, apiKey)
    available = requestResults.get("availableDownloads", [])

    print(f"\n{len(available)} files ready for download.")
    os.makedirs(out_dir, exist_ok=True)

    for item in available:
        downloadFile(item['url'], path=out_dir)

    # Rename .TIF to .tif
    for filename in os.listdir(out_dir):
        infilename = os.path.join(out_dir, filename)
        if os.path.isfile(infilename):
            oldbase, ext = os.path.splitext(filename)
            if ext.upper() == '.TIF':
                os.rename(infilename, os.path.join(out_dir, oldbase + '.tif'))

    print("Download complete.")
