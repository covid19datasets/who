""""""
import requests
import time
from send_log import mail
import argparse
from scrape import scrape
from datetime import datetime
from datetime import date
import pytz


def check_link(http: str):
    """Check that a http link is accessible.

    :http: [str] The link to be checked for accessibility.

    :return: The status ode of the URL.
    """
    r = requests.get(http)
    # The situation reports are produced at different times so poll
    # for the report until it is produced and uploaded!
    if r.status_code is not 200:
        for i in range(48):
            time.sleep(1800)
            r = requests.get(http)
            if r.status_code is 200:
                break

    if r.status_code is not 200:
        mail('FAILURE: Received {}'.format(r.status_code),
             'Failed to connect, received {}'.format(r.status_code) +
             '\nNo file will be uploaded, please retrieve the information'
             'manually!'
             )
        raise ConnectionError


def construct_http(manual_date=None) -> str:
    """Constructs the http for the current day

    :date: [date] A date can be manually entered if needed.

    :return: [str] The current days situation report URL.
    """
    # We want the time in CET because the reports are released at 10:00 CET
    current_datetime = datetime.now(pytz.timezone('CET'))

    # This formats the pytz output replacing the holders with the date.
    if manual_date is None:
        # The first sit rep was on the 21/01/2020, the report number is equal to the
        # number of days since the first report (this may change).
        day = current_datetime.strftime('%m%d')
        report_no = current_datetime.date() - date(2020, 1, 20)
        http = 'https://www.who.int/docs/default-source/coronaviruse/situation-'\
               'reports/2020{}-sitrep-{}-covid-19.pdf'.format(day, report_no.days)
    else:
        report_no = manual_date - date(2020, 1, 20)
        date_string = manual_date.strftime('%Y%m%d')
        http = 'https://www.who.int/docs/default-source/coronaviruse/situation-'\
           'reports/{}-sitrep-{}-covid-19.pdf'.format(date_string, report_no.days)

    return current_datetime.strftime(http)


if __name__ == '__main__':
    # We parse arguments in case manual changes are needed:
    parser = argparse.ArgumentParser(description='Poll for SitRep existence and scrape it to the Github repo.')
    parser.add_argument(
        'date', type=str, help='Date of wanted SitRep in format DDMMYYYY'
                               '(Bare in mind the time is in CET)', default=None
    )
    parser.add_argument(
        'test', type=str, help='For test purposes, it will only change the test branch.', default='No'
    )
    args = parser.parse_args()

    if args.date is None:
        fetch_date = datetime.now(pytz.timezone('CET'))
    else:
        fetch_date = args.date

    http = construct_http(fetch_date)
    check_link(http)
    scrape(http, args.test, args.date)
