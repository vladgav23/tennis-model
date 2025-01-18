import os
import logging
import time
import smart_open
import pandas as pd
import math
import duckdb
from betfairlightweight.resources import MarketBook
from flumine.markets.market import Market
import csv
from pythonjsonlogger import jsonlogger
from unittest.mock import patch
from flumine import utils, clients, FlumineSimulation
from flumine import BaseStrategy
from concurrent import futures

logger = logging.getLogger()

custom_format = "%(asctime) %(levelname) %(message)"
log_handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(custom_format)
formatter.converter = time.gmtime
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)
logger.setLevel(logging.CRITICAL)

class PriceRecorder(BaseStrategy):
    def __init__(self, *args, output_file=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_file = output_file

    def process_new_market(self, market: Market, market_book: MarketBook) -> None:
        market.context['done'] = False

    def check_market_book(self, market, market_book):
        if market.market_type == "MATCH_ODDS" and not market.context['done'] and not market_book.inplay and market_book.status == "OPEN" and market.seconds_to_start <= 300:
            return True

    def process_market_book(self, market, market_book):
        market.context['done'] = True
        market_data = []

        for runner in market_book.runners:
            prices = {
                'market_id': market.market_id,
                'seconds_to_start': market.seconds_to_start,
                'total_matched': runner.total_matched,
                'selection_id': runner.selection_id,
                'atb': utils.get_price(runner.ex.available_to_back, 0),
                'atb_size': utils.get_size(runner.ex.available_to_back, 0),
                'last_traded_price': runner.last_price_traded
            }
            market_data.append(prices)

        # Append market data to a CSV, check if the file exists to decide on the header
        if self.output_file:
            file_exists = os.path.isfile(self.output_file)
            pd.DataFrame(market_data).to_csv(self.output_file, mode='a', header=not file_exists, index=False)


def run_process(markets, output_file):
    try:
        # Set Flumine to simulation mode
        client = clients.SimulatedClient()
        framework = FlumineSimulation(client=client)

        # Remove simulated middleware and add my own
        framework._market_middleware = []

        # Set parameters for our strategy
        strategy = PriceRecorder(
            market_filter={
                "markets": markets,
                "listener_kwargs": {"seconds_to_start": 300, "inplay": False, "cumulative_runner_tv": True}
            },
            output_file=output_file
        )

        # Run our strategy on the simulated market
        with patch("builtins.open", smart_open.open):
            framework.add_strategy(strategy)
            framework.run()

    except UnicodeDecodeError as e:
        # Handle UnicodeDecodeError and log it
        logger.error(f"Unicode decoding error in run_process: {e}")

    except Exception as e:
        # Catch any other unexpected exceptions
        logger.error(f"Unexpected error in run_process: {e}")


# Multi processing
if __name__ == "__main__":
    # Fetch distinct market IDs
    con = duckdb.connect("E:/duckdb/tennis.duckdb", read_only=True)
    market_ids = set(con.execute("SELECT DISTINCT(market_id) FROM base_table WHERE market_id IS NOT NULL").df()[
                         'market_id'].tolist())
    con.close()

    # Scan directory for files
    data_files = []
    directory = 'E:/Data/tennis/betfair-market-files/'

    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith(".bz2"):
                market_id = os.path.splitext(filename)[0]  # Strip .bz2 extension
                if market_id in market_ids:
                    data_files.append(os.path.join(dirpath, filename))

    # Sort files
    data_files = sorted(data_files, key=os.path.basename)

    # All the markets we want to simulate
    processes = os.cpu_count() - 1  # Returns the number of CPUs in the system.
    markets_per_process = 8  # 8 is optimal as it prevents data leakage.

    _process_jobs = []
    output_files = []

    with futures.ProcessPoolExecutor(max_workers=processes) as p:
        # Number of chunks to split the process into depends on the number of markets we want to process and number of CPUs we have.
        chunk = min(markets_per_process, math.ceil(len(data_files) / processes))

        # Split all the markets we want to process into chunks to run on separate CPUs and then run them on the separate CPUs
        for i, process_chunks in enumerate(utils.chunks(data_files, chunk * processes)):
            output_file = f"price-processing/output_process_{i}.csv"  # Each process gets its own output file
            output_files.append(output_file)

            # Submit all chunks for this process as a single job
            _process_jobs.append(
                p.submit(
                    run_process,
                    markets=process_chunks,
                    output_file=output_file
                )
            )

        for job in futures.as_completed(_process_jobs):
            job.result()  # wait for result

    # Combine all the output CSVs into a single DataFrame
    final_df = pd.concat([pd.read_csv(f) for f in output_files if os.path.exists(f)], ignore_index=True)

    # Save the combined result to a final CSV
    final_df.to_csv('final_output.csv', index=False)