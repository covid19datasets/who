""""""
import requests
import time
from send_log import mail
import argparse
from scrape import scrape
import pytz
from datetime import datetime
from datetime import date
import logging
import os
import sys
import traceback
import stat
import shutil


def check_link(http: str):
    """Check that a http link is accessible.

    :http: [str] The link to be checked for accessibility.

    :return: The status ode of the URL.
    """
    r = requests.get(http)
    # The situation reports are produced at different times so poll
    # for the report until it is produced and uploaded!
    if r.status_code != 200:
        for i in range(48):
            time.sleep(1800)
            r = requests.get(http)
            if r.status_code == 200:
                break

    if r.status_code != 200:
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


def remove_readonly(func, path, exc):
    """Remove a readonly directory on unix."""
    # ensure parent directory is writeable too
    pardir = os.path.abspath(os.path.join(path, os.path.pardir))
    if not os.access(pardir, os.W_OK):
        os.chmod(pardir, stat.S_IRWXU| stat.S_IRWXG| stat.S_IRWXO)

    os.chmod(path, stat.S_IRWXU| stat.S_IRWXG| stat.S_IRWXO)  # 0777
    func(path)


if __name__ == '__main__':
    # We parse arguments in case manual changes are needed:
    parser = argparse.ArgumentParser(description='Poll for SitRep existence and scrape it to the Github repo.')
    parser.add_argument(
        '--token',
        type=str,
        help='Token for github.'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Date of wanted SitRep in format DDMMYYYY (Bare in mind the time is in CET)',
        default='None',
        nargs='?',
        const=1
    )
    parser.add_argument(
        '--branch',
        type=str,
        help='For test purposes, it will only change the test branch.',
        default='test',
        nargs='?',
        const=1
    )

    args = parser.parse_args()

    if args.token is None:
        raise ValueError('A Github Token is required!!!')

    if args.date == 'None':
        scrape_date = datetime.now(pytz.timezone('CET')).date()
    else:
        scrape_date = date(
            day=int(args.date[:2]),
            month=int(args.date[2:4]),
            year=int(args.date[4:])
        )

    http = construct_http(scrape_date)
    check_link(http)
    try:
        countries = scrape(http, args.branch, scrape_date, args.token)
        if len(countries['new_countries']) == 0:
            countries['new_countries'] = 'None'
        if len(countries['old_countries']) == 0:
            countries['old_countries'] = 'None'
        mail(
            '{} Success!'.format(scrape_date.strftime('%d%m%Y')),
            'The situation report for {} '.format(scrape_date.strftime('%d%m%Y')) +
            'Was successfully scraped and uploaded. Please review the attached logs!'
            '\n\nThe following are potentially missing or erroneous entries:'
            '\nNew Country names found:\n\t{}'.format(countries['new_countries']) +
            '\nCountries missing:\n\t{}'.format(countries['old_countries'])
        )
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        fname = os.path.split(exc_traceback.tb_frame.f_code.co_filename)[1]
        msg = (
            'The situation report for {} '.format(scrape_date.strftime('%d%m%Y')) +
            'has failed to be scraped and uploaded. Please review the attached logs!'
            '\nException:\n{}\n{}'.format(
                e,
                traceback.print_exception(exc_type, exc_value, exc_traceback))
        )
        mail(
            '{} Failure! {}'.format(scrape_date.strftime('%d%m%Y'), e),
            msg
        )
        print(msg)
    finally:
        # Cleanup:
        shutil.rmtree(scrape_date.strftime('%d%m%Y'), ignore_errors=False, onerror=remove_readonly)
        logging.shutdown()
        os.remove('.log')