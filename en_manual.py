"""Script to manually pull data from e-activist.com and push them into a mysql
database."""

import bs4
import lxml
import logging

import datetime as dt
import requests as req
import mysql.connector as con

# # TODO:
# 1) set up logger + output
# 2) command line arguments for custom time interval
# 3) external file(s) for credentials
# 4) error handling for requested content, i.e. no data
# 5) format the query properly
