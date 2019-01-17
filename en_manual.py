"""Script to manually pull data from e-activist.com and push them into a mysql
database."""

import bs4
import lxml
import json
import logging

import datetime as dt
import requests as req
import mysql.connector as con

# # TODO:
# [X] 1) set up logger + output
# [X] 2) command line arguments for custom time interval
# [X] 3) external file(s) for credentials
# [ ] 4) error handling for requested content, i.e. no data
# [ ] 5) format the query properly

# argparse
parser = argparse.ArgumentParser()
parser.add_argument("--start", type=str,
                    help="Specify the starting date for data collection in"
                         " MMDDYYY format.")
parser.add_argument("--end", type=str,
                    help="Specify the ending date for data collection in"
                         " MMDDYYY format.")
parser.add_argument("-c", "--config", type=str,
                    help="Specify the path to configuration file, containing"
                         " a tokenstring and the MySQL server parameters.")
parser.add_argument("-s", "--silent", action="store_true",
                    help="No progress output to console, only critical "
                    "errors will be printed.")
parser.add_argument("-l", "--log", type=str, help="Specify a logfile.")
args = parser.parse_args()

# loading the config -------------------------------------------------------
config = "config.json" if args.config is None else args.config

with open(config, "r") as conf:
    config = json.load(conf)

# logger setup -------------------------------------------------------------
# create logger instance
logger = logging.getLogger("engagingNetworks")
logger.setLevel(logging.DEBUG)  # general logging level

if args.silent:
    logger.setLevel(logging.CRITICAL)

ch = logging.StreamHandler()  # channel -> console
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t"
                              "%(message)s")

ch.setFormatter(formatter)

# add handlers to logger
logger.addHandler(ch)

if args.log:  # if an additional file logger is specified
    fh = logging.FileHandler(args.log)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
