"""Script to manually pull data from e-activist.com and push them into a mysql
database."""

import bs4
import json
import logging
import argparse

import datetime as dt
import requests as req
import mysql.connector

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

# loading the config ----------------------------------------------------------
config = "config.json" if args.config is None else args.config

with open(config, "r") as conf:
    config = json.load(conf)

# logger setup ----------------------------------------------------------------
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


# helper functions ------------------------------------------------------------
def soupify(token: str, startDate: str, endDate: str,
            configTypes: str, contentType: str = "xml") -> bs4.BeautifulSoup:
    """Call the enganging networks dataservice for the given time interval.
    No error checking/handling is done in here.

    Parameters
    ----------
    token : str
        `token` is needed for authentication.
    startDate : str
        The `startDate` is inclusive in the format MMDDYYYY.
    endDate : str
        The `endDate` is exclusive in the format MMDDYYYY (except if
        `startDate` and endDate are equal).
    configTypes : str
        The `configTypes` are e.g. "PET" or "QCB".
    contentType : str
        The `contentType` is e.g. "xml".

    Returns
    -------
    bs4.BeautifulSoup
        A BeautifulSoup instance with the requested url's text and contentType.

    """
    # formatting the url (for python < 3.7)
    url = ("https://www.e-activist.com/ea-dataservice/export.service?token={}"
           "&startDate={}&endDate={}&type={}&configTypes={}"
           "".format(token, startDate, endDate, contentType, configTypes))

    response = req.get(url)
    return bs4.BeautifulSoup(response.text, contentType)


def query(dbcon: mysql.connector.connect, soup: bs4.BeautifulSoup,
          configTypes: str) -> None:
    """Upload the contents of `soup` using the `dbcon` instance.

    Parameters
    ----------
    dbcon : mysql.connector.connect
        `dbcon` is the instance created with given credentials.
    soup : bs4.BeautifulSoup
        `soup` contains all the data for the generated url string.

    """
    def querySelector(d: dict) -> str:
        """Build the right query depending on the contents of `d`.

        Parameters
        ----------
        d : dict
            `d` contains the content of a row from the engagement network.

        Returns
        -------
        str
            Contains the formatted query string.

        """
        q = ""  # default value
        t = d['type']  # getting the type of content data

        if t == "PET":
            q = ("insert into Lead.engaging_networks (account_id, "
                 "supporter_id, person_id, first_name, middle_name, last_name,"
                 " supporter_email, phone_number, phone_type, opt_in_status, "
                 "city, country, external_reference1, external_reference2, "
                 "external_reference3, lead_type, campaign, utm_source, "
                 "device, email, signing_date, signing_time, "
                 "supporter_create_date, date_of_birth, transfer_time) "
                 "values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', "
                 "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', "
                 "'{}', '{}', '{}', '{}', '{}', '{}', now())"
                 "".format(d.get("account_id", "12"),
                           d.get("supporter_id", " "),
                           d.get("person_id", " "),
                           d.get("first_name", " ").replace("'", "\\'"),
                           d.get("middle_name", " ").replace("'", "\\'"),
                           d.get("last_name", " ").replace("'", "\\'"),
                           d.get("supporter_email", " ").replace("'", "\\'"),
                           d.get("phone_number", " ").replace("'", "\\'"),
                           d.get("phone_type", " "),
                           d.get("opt_in_status", " "),
                           d.get("city", " ").replace("'", "\\'"),
                           d.get("country", " ").replace("'", "\\'"),
                           d.get("external_reference1", " ").replace("'",
                                                                     "\\'"),
                           d.get("external_reference2", " ").replace("'",
                                                                     "\\'"),
                           d.get("external_reference3", " ").replace("'",
                                                                     "\\'"),
                           d.get("type", " ").replace("'", "\\'"),
                           d.get("id", " ").replace("'", "\\'"),
                           d.get("data33", " ").replace("'", "\\'"),
                           d.get("data32", " ").replace("'", "\\'"),
                           d.get("email", " ").replace("'", "\\'"),
                           d.get("date", " "),
                           d.get("time", " "),
                           d.get("supporter_create_date", " "),
                           d.get("date_of_birth", " ").replace("'", "\\'")))

        if t == "QCB":
            supporter = int(d.get("supporter_id", " "))
            status = d.get("id", " ")

            # dictionary for easier string formatting
            field = {"email_ok": "email",
                     "sms_ok": "sms",
                     "phone_ok": "phone"}

            f = field.get(status, "")
            if f != "":  # if status is in field dictionary: build query
                q = ("update Lead.engaging_networks set {}_status='{}' where "
                     "supporter_id='{}' and transfer_time>=date(now())"
                     "".format(f, status, supporter))

        return q

    # create cursor
    cursor = dbcon.cursor()

    for row in soup.rows:
        if row != "\n":
            content = dict()
            for child in row:
                name = child.name
                if name is not None:
                    content[name] = child.string

            # generate the query string depending on configType (PET, QCB)
            q = querySelector(content)
            cursor.execute(q)
            dbcon.commit()

    # close cursor
    cursor.close()


def createIntervals(start: str, end: str, formatter: str = "%m%d%Y") -> list:
    """Create a temporally ordered list of dates from `start` to `end`
    including both.

    Parameters
    ----------
    start : str
        `start` date in given format.
    end : str
        `end` date in given format.
    formatter : str
        `formatter` describes the input of the `start` and `end` string and
        defines the format of the output string-list.

    Returns
    -------
    list
        A list of datestrings from `start` to `end` including both.

    """
    s = dt.datetime.strptime(start, formatter)
    e = dt.datetime.strptime(end, formatter)

    diff = e - s  # calculate the time difference in days
    days = []
    for d in range(diff.days + 1):
        newdate = s + dt.timedelta(days=d)
        days.append(newdate.strftime(formatter))  # append formatted dates

    return days


# main function ---------------------------------------------------------------
def main() -> None:
    """Iterate over the time interval between `start` and `end` and call the
    above defined functions for both config types (PET, QCB) and for each day
    individually.
    If no command line arguments for `start` and `end` are given, the default
    values for both are <today - 1>. Although before that, a query is executed
    to get the most recent entry date in the db. If the most recent entry + one
    day is older than <today - 1>, the `start` date is adjusted to <most recent
    + 1>.

    """
    # MySQL connector setup
    logger.info("Creating mysql.connector instance now...")
    dbcon = mysql.connector.connect(**config['mysql'])

    # setting time interval values
    formatter = "%m%d%Y"  # weird american date order. TODO: cli?
    start = args.start
    end = args.end

    if start is None:
        # checking if older data needs to be pulled in as well
        timequery = ("SELECT signing_date FROM Lead.engaging_networks ORDER BY"
                     " signing_date DESC LIMIT 1")  # date of most recent entry

        cursor = dbcon.cursor()  # initialise cursor
        cursor.execute(timequery)
        [(querydate, )] = list(cursor)  # unpacks the single entry
        logger.info("Last entry in MySQL was on %s.", querydate)
        querydate += dt.timedelta(days=1)  # starting day for new data

        # get the data from the day before -> today's data is not fully ready
        start = dt.datetime.today() - dt.timedelta(days=1)
        if querydate < start.date():  # check if older data needs pulling
            start = querydate

        start = start.strftime(formatter)

    if end is None:
        end = dt.datetime.today() - dt.timedelta(days=1)
        end = end.strftime(formatter)  # weird american date order

    logger.info("Proceeding with starting date %s and ending date %s.", start,
                end)

    # main loop
    for configType in ["PET", "QCB"]:
        for day in createIntervals(start, end):
            # output
            logger.info("[%s] Working on day [%s] now.", configType, day)

            # getting content
            soup = soupify(token=config['token'], startDate=day, endDate=day,
                           configTypes=configType)

            # error handling
            errors = soup.findAll("error")
            if errors:
                for e in errors:
                    logger.error("[%s] Error while collecting data from "
                                 "engaging network: %s", configType, e)

            else:  # no errors, proceed normally
                query(dbcon=dbcon, soup=soup, configTypes=configType)

    # close connector at the end
    dbcon.close()


# actually running the code ---------------------------------------------------
if __name__ == "__main__":
    main()
