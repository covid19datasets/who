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


def construct_http(fetch_date) -> str:
    """Constructs the http for the current day

    :date: [date] A date can be manually entered if needed.

    :return: [str] The current days situation report URL.
    """
    # We want the time in CET because the reports are released at 10:00 CET
    current_datetime = datetime.now(pytz.timezone('CET'))
    report_no = fetch_date - date(2020, 1, 20)
    date_string = fetch_date.strftime('%Y%m%d')
    http = 'https://www.who.int/docs/default-source/coronaviruse/situation-'\
       'reports/{}-sitrep-{}-covid-19.pdf'.format(date_string, report_no.days)

    return current_datetime.strftime(http)


if __name__ == '__main__':
    # We parse arguments in case manual changes are needed:
    parser = argparse.ArgumentParser(description='Poll for SitRep existence and scrape it to the Github repo.')
    parser.add_argument(
        '--date', type=str, help='Date of wanted SitRep in format DDMMYYYY'
                               '(Bare in mind the time is in CET)', default='None', nargs='?'
    )
    parser.add_argument(
        '--test', type=str, help='For test purposes, it will only change the test branch.', default='No', nargs='?'
    )
    args = parser.parse_args()

    if args.date is 'None':
        scrape_date = datetime.now(pytz.timezone('CET')).date()
    else:
        scrape_date = date(
            day=int(args.date[:2]),
            month=int(args.date[2:4]),
            year=int(args.date[4:])
        )

    from datetime import datetime
    from datetime import timedelta
    from datetime import date
    start = date(
        day=2,
        month=3,
        year=2020
    )
    today = datetime.now(pytz.timezone('CET')).date()
    today += timedelta(days=-1)
    for i in range(31):
        print(today.strftime('%d%m%Y'))
        http = construct_http(today)
        check_link(http)
        try:
            scrape(http, 'no', today.strftime('%d%m%Y'))
        except:
            with open('broke.txt', 'a') as file:
                file.write(today.strftime('%d%m%Y'))
                file.write('\n')
        today += timedelta(days=-1)

    #http = construct_http(scrape_date)
    #check_link(http)
    #scrape(http, args.test, scrape_date.strftime('%d%m%Y'))
    #mail(
    #    '{} Success!'.format(scrape_date.strftime('%d%m%Y')),
    #    'The situation report for {}'.format(scrape_date.strftime('%d%m%Y')) +
    #    'Was successfully scraped and uploaded. Please review the attached logs!'
    #)