# Import libraries
import time
import logging

import keyring
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
# from strategies.strategy import BackLayModel
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
LOGNAME = datetime.now().strftime('%Y%m%d_%H%M')
STAKE_UNIT = 1
TEST_DATA_PATH = 'E:/Data/tennis/betfair-market-files/' # only need if RUN_TYPE is 'test'
MAX_TTJ = 300
MAX_BACK_PRICE = 15

def run_process(run_type, markets):
    client = clients.SimulatedClient()
    framework = FlumineSimulation(client=client)

    market_filter = {
        "markets": markets,
        'market_types': ['WIN'],
        "listener_kwargs": {"inplay": False, "seconds_to_start": MAX_TTJ, "cumulative_runner_tv": True},
    }

    client.min_bet_validation = False

    with patch('builtins.open', smart_open.open):
        framework.add_strategy(
            # BackLayModel(
            #     market_filter=market_filter,
            #     max_trade_count=2,
            #     stake_unit=STAKE_UNIT,
            #     max_back_price=MAX_BACK_PRICE,
            #     max_selection_exposure=STAKE_UNIT*MAX_BACK_PRICE,
            #     max_order_exposure=STAKE_UNIT*MAX_BACK_PRICE,
            #     max_seconds_to_start=MAX_TTJ,
            #     run_type=run_type
            # )
        )
        framework.add_logging_control(
            OrderRecorder(
                logname=LOGNAME
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

    random.shuffle(data_files)

    processes = 8  # Returns the number of CPUs in the system.
    markets_per_process = 8   # 8 is optimal as it prevents data leakage.

    # run_process(RUN_TYPE,data_files[:40])

    chunk = min(
        markets_per_process, math.ceil(len(data_files) / processes)
    )

    _process_jobs = []
    with futures.ProcessPoolExecutor(max_workers=processes) as p:
        for m in (utils.chunks(data_files, chunk)):
            _process_jobs.append(
                p.submit(
                    run_process,
                    markets=m
                )
            )
        for job in futures.as_completed(_process_jobs):
            job.result()  # wait for result