# Import libraries
import time
import logging

import smart_open
import itertools
import os
import pandas as pd
import random
import math
import betfairlightweight
import glob

from betfairlightweight.filters import streaming_market_filter
from concurrent import futures
from unittest.mock import patch
from flumine import clients, FlumineSimulation, utils
from pythonjsonlogger import jsonlogger
from datetime import datetime

from flumine.flumine import Flumine
from middleware import GetPricesFromScoredHoldout
from strategies.strategy import TennisH2H
from logging_controls.logging_controls import OrderRecorder

# Logging
logger = logging.getLogger()
custom_format = "%(asctime) %(levelname) %(message)"
log_handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(custom_format)
formatter.converter = time.gmtime
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)
logger.setLevel(logging.CRITICAL)

# Params
MODEL_NAME = '20241019_094247'
STAKE_UNIT = 10
TEST_DATA_PATH = 'E:/Data/tennis/betfair-market-files/' # only need if RUN_TYPE is 'test'
MAX_TTJ = 1
# MAX_BACK_PRICE = 15

def run_process(markets, preds_df):
    client = clients.SimulatedClient()
    framework = FlumineSimulation(client=client)

    market_filter = {
        "markets": markets,
        'market_types': ['MATCH_ODDS'],
        "listener_kwargs": {"inplay": False, "seconds_to_start": MAX_TTJ, "cumulative_runner_tv": True},
    }

    client.min_bet_validation = False

    with patch('builtins.open', smart_open.open):
        framework.add_market_middleware(
            GetPricesFromScoredHoldout(preds_df)
        )
        framework.add_strategy(
            TennisH2H(
                market_filter=market_filter,
                max_trade_count=1,
                stake_unit=STAKE_UNIT,
                max_selection_exposure=STAKE_UNIT,
                max_order_exposure=STAKE_UNIT,
            )
        )
        framework.add_logging_control(
            OrderRecorder(
                logname=MODEL_NAME
            )
        )
        framework.run()

# Multi processing
if __name__ == "__main__":
    # Run the API in a terminal
    data_files = []

    for dirpath, dirnames, filenames in itertools.chain(os.walk(TEST_DATA_PATH)):
        for filename in filenames:
            data_files.append(os.path.join(dirpath, filename))

    data_files = sorted(data_files, key=os.path.basename)

    # Read the necessary columns from the CSV
    model_preds = pd.read_csv(f'../model/outputs/{MODEL_NAME}/simulation_file.csv')#.query('tournament_category in ("ATP","Challenger")')
    model_preds.drop(columns=[x for x in model_preds if x.endswith('_avg')], inplace=True)

    # Convert selection_id to int and market_id to a string with zero padding on the right
    model_preds['selection_id_home'] = model_preds['selection_id_home'].astype(int)
    model_preds['selection_id_away'] = model_preds['selection_id_away'].astype(int)
    model_preds['market_id'] = model_preds['market_id'].astype(str).str.pad(9, fillchar='0', side='right')

    unique_market_ids = set(model_preds['market_id'].unique().tolist())

    data_files = [x for x in data_files if os.path.basename(x).rstrip(".bz2") in unique_market_ids]

    processes = 8  # Returns the number of CPUs in the system.
    markets_per_process = 8   # 8 is optimal as it prevents data leakage.

    # run_process(data_files[:40],final_df)

    chunk = min(
        markets_per_process, math.ceil(len(data_files) / processes)
    )

    _process_jobs = []
    with futures.ProcessPoolExecutor(max_workers=processes) as p:
        for m in (utils.chunks(data_files, chunk)):
            _process_jobs.append(
                p.submit(
                    run_process,
                    markets=m,
                    preds_df=model_preds
                )
            )
        for job in futures.as_completed(_process_jobs):
            job.result()  # wait for result