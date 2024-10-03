import os
import requests
import pandas as pd
from multiprocessing import Pool
import json
import time
from datetime import datetime, timedelta

class PersistentDownloader:
    def __init__(self):
        self.session = requests.Session()
        self.download_count = 0
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'Referer': 'https://www.sofascore.com/'
        }

    def download_data(self, date):
        if self.download_count >= 20:
            self.refresh_connection()

        url = f'https://api.sofascore.com/api/v1/sport/tennis/scheduled-events/{date}'
        savefilename = f'tennis_events_{date}.json'
        savepath = os.path.join('E:/', 'Data', 'tennis', 'sofascore', 'events', savefilename)

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
                    print(f"Failed to get data for {date}. Status code: {response.status_code}")
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

def download_worker(date_list):
    downloader = PersistentDownloader()
    for date in date_list:
        downloader.download_data(date)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def get_existing_dates(folder_path):
    existing_dates = set()
    for filename in os.listdir(folder_path):
        if filename.startswith('tennis_events_') and filename.endswith('.json'):
            date_str = filename[14:24]  # Extract date from filename
            existing_dates.add(date_str)
    return existing_dates

def main():
    # Define the output folder
    output_folder = os.path.join('E:/', 'Data', 'tennis', 'sofascore', 'events')

    # Ensure the directory exists
    os.makedirs(output_folder, exist_ok=True)

    # Get existing dates
    existing_dates = get_existing_dates(output_folder)

    # Define the date range
    start_date = datetime(2014, 1, 1)
    end_date = datetime(2024, 9, 25)  # Adjust this to your desired end date

    # Generate a list of dates, excluding existing ones
    date_list = [
        date.strftime("%Y-%m-%d")
        for date in daterange(start_date, end_date)
        if date.strftime("%Y-%m-%d") not in existing_dates
    ]

    if not date_list:
        print("All dates in the specified range have already been downloaded. No new downloads needed.")
        return

    print(f"Downloading data for {len(date_list)} new dates.")

    num_processes = 8
    tasks_per_process = len(date_list) // num_processes + 1
    chunks = [date_list[i:i + tasks_per_process] for i in range(0, len(date_list), tasks_per_process)]

    with Pool(num_processes) as pool:
        pool.map(download_worker, chunks)

if __name__ == "__main__":
    main()