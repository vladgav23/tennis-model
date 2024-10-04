import os
import requests
import pandas as pd
from multiprocessing import Pool
import json
import time
import duckdb

class PersistentDownloader:
    def __init__(self):
        self.session = requests.Session()
        self.download_count = 0
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'Referer': 'https://www.sofascore.com/'
        }

    def download_data(self, eventId):
        if self.download_count >= 100:
            self.refresh_connection()

        url = f'https://api.sofascore.com/api/v1/event/{eventId}/point-by-point'
        savefilename = f'pbp_{eventId}.json'
        savepath = os.path.join('E:/', 'Data', 'tennis', 'sofascore', 'point-by-point-itf', savefilename)

        if os.path.exists(savepath):
            print(f"Skipped: {savefilename} (already exists)")
            return

        max_retries = 3
        delay = 5

        for attempt in range(max_retries):
            try:
                response = self.session.get(url, headers=self.headers, timeout=30)
                if response.status_code == 200:
                    with open(savepath, 'w') as f:
                        json.dump(response.json(), f, indent=4)
                    print(f"Response saved as '{savefilename}'.")
                    self.download_count += 1
                    return
                else:
                    print(f"Failed to get data for {eventId}. Status code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    print(f"Error downloading {url}: {str(e)}. Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    print(f"Failed to download {url} after {max_retries} attempts: {str(e)}")

    def refresh_connection(self):
        self.session.close()
        self.session = requests.Session()
        self.download_count = 0
        print("Refreshed connection")

def download_worker(event_ids):
    downloader = PersistentDownloader()
    for event_id in event_ids:
        downloader.download_data(event_id)

def get_existing_event_ids(folder_path):
    existing_ids = set()
    for filename in os.listdir(folder_path):
        if filename.startswith('pbp_') and filename.endswith('.json'):
            event_id = filename[4:-5]  # Extract event ID from filename
            existing_ids.add(int(event_id))
    return existing_ids

def main():
    # Define the output folder
    output_folder = os.path.join('E:/', 'Data', 'tennis', 'sofascore', 'point-by-point-itf')

    # Ensure the directory exists
    os.makedirs(output_folder, exist_ok=True)

    con = duckdb.connect("E:/duckdb/tennis.duckdb", read_only=True)

    ids_to_get = con.execute("""
    SELECT DISTINCT match_id
    FROM sofascore_match_stats m
    INNER JOIN sofascore_events e 
    ON m.match_id = e.id

    WHERE e.tournament_category IN ('ITF Men','ITF Women')
    """).df()

    con.close()

    ids_list = ids_to_get['match_id'].to_list()

    # Get existing event IDs
    existing_ids = get_existing_event_ids(output_folder)

    # Filter out existing IDs
    new_ids = [id for id in ids_list if id not in existing_ids]

    if not new_ids:
        print("All event IDs have already been downloaded. No new downloads needed.")
        return

    print(f"Downloading data for {len(new_ids)} new event IDs.")

    num_processes = 8
    tasks_per_process = len(new_ids) // num_processes + 1
    chunks = [new_ids[i:i + tasks_per_process] for i in range(0, len(new_ids), tasks_per_process)]

    with Pool(num_processes) as pool:
        pool.map(download_worker, chunks)

if __name__ == "__main__":
    main()